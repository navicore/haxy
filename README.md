<p align="center">
<code>                                                 
░██    ░██     ░████    ░██    ░██ ░██    ░██
░██    ░██    ░██ ░██    ░██  ░██   ░██  ░██
░██    ░██   ░██   ░██    ░█████     ░█████
░█████████  ░█████████     ░███       ░███
░██    ░██ ░██      ░██    ░████      ░██
░██    ░██ ░██      ░██   ░██ ░██     ░██
░██    ░██ ░██      ░██  ░██   ░██    ░██
                                                 </code></p>

You're looking at Haxy, a new git forge. This is a work in progress...there's not much here yet besides hopes and dreams. We're about to do for git forges what Bill Hicks did for comedy, Earth Crisis did for hardcore music, and Marvin Heemeyer did for exterior remodeling. I'm not sure what any of that means but it sounded cool. The point is, strap yourselves in...we're gonna Leeroy Jenkins our way through this!

## How to fire this puppy up and get 'er done

To build, install zig 0.16.0 and do `zig build` and you'll find the binary at `zig-out/bin/haxy`.

It can't do much right now...it's just a git server at the moment. If you want to try it out, do this:

```
mkdir -p server
./zig-out/bin/haxy serve --http-listen 127.0.0.1:8080 --data-dir server
```

Then, in another terminal, do this:

```
mkdir -p client/test
cd client/test
git init
echo "hello" > hello.txt
git add hello.txt
git commit -m "let there be light"
git remote add origin http://127.0.0.1:8080/test
git push origin HEAD:master
```

After that, you'll see your repo in `server/repos/test`. It's MAGIC! Obviously, this is not very useful right now. I'm currently nailing down the git functionality before working on other things like user accounts and the UI.

## The longer term vision

A few [early design ideas](https://www.youtube.com/watch?v=9YjWGXi5tDw):

1. Store project metadata (issues, pull requests, and discussions) in the repo, so it can be easily replicated to different Haxy instances.
2. Provide a TUI that can be served over SSH, in addition to a web UI.

*"C'mon Alex! You always dreamt about going on a big adventure! Let this be our first!" -- Lunar: Silver Star Story*
