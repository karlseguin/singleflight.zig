Duplicate function call suppression for Zig

```zig
const singleflight = @import("singleflight");

// group is thread-safe
const group = singleflight.Group(*User).init(allocator);
defer group.deinit();

// multiple threads can call group.load on the same key
const result = try group.load(UserLoadState, "user:4", &loadUser, UserLoadState{.id = 4});


// result.value is the value returned by loadUser
// result.caller is true when this is the thread that called loadUser
// result.last is true when all waiting threads for this key have unblocked
...

const UserLoadState = struct {
    id: u32,
}

fn loadUser(state: UserLoadState, _key: []const u8) !User {
    // load the user, using either the app-specific state
    // or the key
    // ...
    return user;
}
```

An application-specific "state" is provided to the `group.load` function and then passed back to the callback function. This allows the application to pass itself information necessary to load the object.

This library is not a cache. Assume thread 1, thread 2 and thread 3 all attempt to load "user:1". It is possible for one, two or all three threads to call `loadUser`. This is more likely when the callback, `loadUser` in this case, executes quickly. This happens because the Singleflight group tracks threads waiting for the result and cleans itself when the last blocked thread gets the result (on a per-key basis). Thread 1 can call `loadUser`, get the result before thread 2's intent in the key is registered. In this case, thread 1 will clean up the group and return the value. 

In other words, this library is not a cache. See [cache.zig](https://github.com/karlseguin/cache.zig) if you want a cache (combining the two libraries makes sense).
