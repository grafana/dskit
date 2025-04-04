## Build local cluster using `Ring`

This examples uses `loopback` interface to build a ring of multiple processes locally.

### Usage

1. Build
```
go build local.go
```

2. Start a peer

```
./local -bindaddr=127.0.0.1
```

3. Start another peer in different terminal

```
./local -bindaddr=127.0.0.2 -join-member=127.0.0.1
```

> #### NOTE for MacOS users
> you will need to setup a network alias for each `bindaddr` other than `127.0.0.1` you want to use. For example, to create a second instance listening on `127.0.0.2` you can run the following command to enable it: `sudo ifconfig lo0 alias 127.0.0.2 up`

4. You can start as many peers as you want with different loopback bindaddr.

5. Check the ring page

Go to ring page of one of the peers. Each peer's ring page should show all the peers in the ring. Should look something like below.

e.g: http://127.0.0.1:8100/ring

![Ring Status Page](./images/local-ring.png)

6. Also check the memberlist status page.

e.g: http://127.0.0.1:8100/kv

![Memberlist Status Page](./images/local-memberlist.png)

7. You can also get the `ring` information as a client.

```
./local -mode=client
```
