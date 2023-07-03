const std = @import("std");

const Mutex = std.Thread.Mutex;
const Allocator = std.mem.Allocator;

pub fn Group(comptime T: type) type {
	return struct {
		mutex: Mutex,
		jobs: std.StringHashMap(Job(T)),

		const Self = @This();

		pub fn init(allocator: Allocator) Self {
			return .{
				.mutex = .{},
				.jobs = std.StringHashMap(Job(T)).init(allocator),
			};
		}

		pub fn deinit(self: *Self) void {
			self.jobs.deinit();
		}

		pub fn load(self: *Self, comptime S: type, key: []const u8, loader: *const fn(state: S, key: []const u8) anyerror!T, state: S) !Result(T) {
			self.mutex.lock();
			const gop = self.jobs.getOrPut(key) catch |err| {
				// don't use errdefer here instead of this, since addObserver can also
				// return an error, but it will already have unlocked the mutext.
				self.mutex.unlock();
				return err;
			};

			if (gop.found_existing) {
				// addObserver will release the mutex lock
				// this blocks until the result is available
				return gop.value_ptr.addObserver(self);
			}

			gop.value_ptr.* = Job(T).init(key);
			self.mutex.unlock();
			return gop.value_ptr.run(S, self, loader, state);
		}
	};
}

fn Job(comptime T: type) type {
	return struct {
		// the key for this job
		key: []const u8,

		// number of callers waiting for the result
		observers: u32,

		// used to signal observers that result is available
		cond: std.Thread.Condition,

		mutex: Mutex,

		// the result (either T or an error)
		result: ?JobResult,

		const Self = @This();

		const JobResult = union(enum) {
			ok: T,
			err: anyerror,
		};

		fn init(key: []const u8) Self {
			return .{
				.key = key,
				.cond = .{},
				.mutex = .{},
				.result = null,
				.observers = 1,
			};
		}

		fn run(self: *Self, comptime S: type, group: *Group(T), loader: *const fn(state: S, key: []const u8) anyerror!T, state: S) !Result(T) {
			const result: JobResult = load_result: {
				const value = loader(state, self.key) catch |err| {
					break :load_result .{.err = err};
				};
				break :load_result .{.ok = value};
			};

			self.mutex.lock();
			self.result = result;
			const last = self.notifyNextObserver(group);
			// when last == true, self is no longer valid!

			switch (result) {
				.err => |err| return err,
				.ok => |value| return .{.value = value, .caller = true, .last = last},
			}
		}

		// This can only be called under a group.mutex.lock
		fn addObserver(self: *Self, group: *Group(T)) !Result(T) {
			var mutex = &self.mutex;
			mutex.lock();

			// Now that we've locked the job, we can unlock the main group container.
			group.mutex.unlock();

			// There's a very small window of opportunity where the result can be
			// availble and the job still in the group.jobs lookup.
			if (self.result) |result| {
				mutex.unlock();
				return switch(result) {
					.err => |err| err,
					.ok => |value| .{.value = value, .caller = false, .last = false},
				};
			}

			// increment the observer count
			self.observers += 1;

			var result: JobResult = undefined;
			while (true) {
				// this releases the mutex and blocks until a different thread calls
				// cond.signal;
				self.cond.wait(mutex);
				// wait can unblock "at random" even without a signal, so we need
				// to double check and make sure we really did get a value

				if (self.result) |r| {
					result = r;
					break;
				}
			}

			// If we're here, we've been signal, the result has to be available
			// the job.mutex has automatically been locked by cond.
			const last = self.notifyNextObserver(group);
			// when last == true, self is no longer valid!

			return switch(result) {
				.err => |err| err,
				.ok => |value| .{.value = value, .caller = false, .last = last},
			};
		}

		// self.mutex is locked when this function is called
		fn notifyNextObserver(self: *Self, group: *Group(T)) bool {
			const remaining_observers = self.observers - 1;

			if (remaining_observers > 0) {
				// We still have some observers. Our job is simple: we just notify one
				// that the results are ready.
				self.observers = remaining_observers;
				self.mutex.unlock();
				self.cond.signal();
				return false;
			}

			// We were the last observer. We need to do some cleanup.
			// it's important that the job mutex is locked while we do this in order
			// to prevent another thread from slipping in and getting this job,
			// which we're about to


			// to avoid deadlocks, we always have to grap the job mutex after the
			// group mutex
			self.mutex.unlock();
			group.mutex.lock();
			self.mutex.lock();

			// why aren't we checking self.observers again?  Because if another job
			// did slip in, it would see the result is available and returning it
			// directly, without a need for signal.

			std.debug.assert(group.jobs.remove(self.key));
			group.mutex.unlock();

			// This function _has_ to be called under self.mutex.lock, so you're
			// probably thinking we _have_ to call self.mutex.unlock() here. But...
			// that won't work because "self" is no longer valid - it was removed
			// from the map, which is its owner. We could solve this by allocating
			// jobs on the heap, but we don't actually have to. Like I said, Job
			// is no longer valid, no one should be referencing it anymore, so why
			// do we care about unlocking the mutex? Just return!
			return true;
		}
	};
}

fn Result(comptime T: type) type {
	return struct {
		value: T,
		last: bool,
		caller: bool,
	};
}

const t = std.testing;
test "simple/sanity success" {
	var group = Group(TestUser).init(t.allocator);
	defer group.deinit();

	const result = try group.load(TestUserState, "user1", &loadUser, .{.id = 3});
	try t.expectEqual(true, result.caller);
	try t.expectEqual(true, result.last);
	try t.expectEqual(@as(i32, 3), result.value.id);
	try t.expectEqualStrings("user1", result.value.key);
}

test "simple/sanity error" {
	var group = Group(TestUser).init(t.allocator);
	defer group.deinit();
	try t.expectError(error.Error1, group.load(TestUserState, "user2", &loadUser, .{.id = -1}));
}

const TestUser = struct {
	key: []const u8,
	id: i32,
};

const TestUserState = struct {
	id: i32,
};

fn loadUser(state: TestUserState, key: []const u8) !TestUser {
	if (state.id == -1) {
		return error.Error1;
	}

	return .{
		.key = key,
		.id = state.id,
	};
}

// This fuzz test uses random timings to test for possible deadlocks. It cannot
// test the core behavior of the duplicate function call supressions because
// 2 threads for the same key might execute serially (singleflight is not a
// cache, it doesn't keep previous values).
test "fuzz deadlocks" {
	var group = Group(u32).init(t.allocator);
	defer group.deinit();

	// Each testConcurrency itself has concurrency. Our goal is to test that
	// concurrent access to the group for both different keys and the same
	// key works.
	const t1 = try std.Thread.spawn(.{}, testDeadlocks, .{&group});
	const t2 = try std.Thread.spawn(.{}, testDeadlocks, .{&group});
	const t3 = try std.Thread.spawn(.{}, testDeadlocks, .{&group});
	t1.join(); t2.join(); t3.join();
}

fn testDeadlocks(group: *Group(u32)) void {
	var seed: u64 = undefined;
	std.os.getrandom(std.mem.asBytes(&seed)) catch unreachable;
	var r = std.rand.DefaultPrng.init(seed);
	var random = r.random();
	for (0..1000) |_| {
		const value = random.int(u32);

		// 2-10 workers
		const worker_count = random.uintAtMost(u8, 8) + 2;
		var concurrent_test = ConcurrentRunner.init(group, worker_count);

		concurrent_test.run(TestConcurrencyState{
			.id = value,
			.random = &random,
		});

		t.expectEqual(worker_count, concurrent_test.result_count) catch unreachable;

		// the captures doesn't matter too much, since the loader always returns
		// value in this case, but might as well check it (what really matters
		// with this test is that it completed (didn't deadlock))
		for (0..worker_count) |i| {
			t.expectEqual(value, concurrent_test.results[i]) catch unreachable;
		}
	}
}

// this fuzz tests makes sure that all threads wait for the first thread to finish
// and return the same result
test "fuzz supression" {
	var group = Group(u32).init(t.allocator);
	defer group.deinit();

	// Each testConcurrency itself has concurrency. Our goal is to test that
	// concurrent access to the group for both different keys and the same
	// key works.
	const t1 = try std.Thread.spawn(.{}, testSupression, .{&group});
	const t2 = try std.Thread.spawn(.{}, testSupression, .{&group});
	const t3 = try std.Thread.spawn(.{}, testSupression, .{&group});
	t1.join(); t2.join(); t3.join();
}

fn testSupression(group: *Group(u32)) void {
	var seed: u64 = undefined;
	std.os.getrandom(std.mem.asBytes(&seed)) catch unreachable;
	var r = std.rand.DefaultPrng.init(seed);
	var random = r.random();
	for (0..1000) |_| {
		// 2-10 workers
		const worker_count = random.uintAtMost(u8, 8) + 2;
		var concurrent_test = ConcurrentRunner.init(group, worker_count);

		concurrent_test.run(TestConcurrencyState{
			.id = null,
			.random = &random,
		});

		t.expectEqual(worker_count, concurrent_test.result_count) catch unreachable;

		// Here the value matters a lot. The loader returned a random value, so we
		// expect all results to have this value to prove that the loader was only
		// called once.
		const value = concurrent_test.results[0];
		for (1..worker_count) |i| {
			t.expectEqual(value, concurrent_test.results[i]) catch unreachable;
		}
	}
}

const ConcurrentRunner = struct {
	mutex: Mutex,
	worker_count: u8,
	results: [10]u32,
	group: *Group(u32),
	result_count: u8,

	fn init(group: *Group(u32), worker_count: u8) ConcurrentRunner {
		return .{
			.mutex = .{},
			.group = group,
			.results = undefined,
			.result_count = 0,
			.worker_count = worker_count,
		};
	}

	fn run(self: *ConcurrentRunner, state: TestConcurrencyState) void {
		const key_id = state.random.int(u32);
		const key = std.fmt.allocPrint(t.allocator, "key:{d}", .{key_id}) catch unreachable;

		var threads: [10]std.Thread = undefined;
		for (0..self.worker_count) |i| {
			threads[i] = std.Thread.spawn(.{}, testConcurrentWorker, .{self, key, state}) catch unreachable;
		}

		for (0..self.worker_count) |i| {
			threads[i].join();
		}
		t.allocator.free(key);
	}
};

fn testConcurrentWorker(runner: *ConcurrentRunner, key: []const u8, state: TestConcurrencyState) void {
	const result = runner.group.load(TestConcurrencyState, key, &testConcurrentLoader, state) catch unreachable;
	runner.mutex.lock();
	const result_count = runner.result_count;
	runner.results[result_count] = result.value;
	runner.result_count = result_count + 1;
	runner.mutex.unlock();
}

// This works with the two fuzz testers. Whe an id is given, then we'll always
// return this id but sleep for a (short) random time. The goal here is to test
// for deadlocks.
// When an id is not given, we'll randomly generateone and sleep for a fixed time
// the goal here is to ensure that the function is only called once per key. For
// this to work, we need to sleep long enough to make sure all threads are started
// before this exits.
fn testConcurrentLoader(state: TestConcurrencyState, _: []const u8) !u32 {
	if (state.id) |id| {
		std.time.sleep(state.random.uintAtMost(u32, 1000));
		return id;
	} else {
		std.time.sleep(100_000);
		return state.random.int(u32);
	}
}

const TestConcurrencyState = struct {
	id: ?u32,
	random: *std.rand.Random,
};
