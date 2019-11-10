module github.com/yyyar/gobetween/main

go 1.12

replace github.com/yyyar/gobetween => ./src

replace github.com/panjf2000/gnet => ./../gnet/

require (
	github.com/panjf2000/gnet v1.0.0-rc.3 // indirect
	github.com/yyyar/gobetween v0.0.0-00010101000000-000000000000
)
