package main

func main() {
	pack := Package{}
	pack.init()
	pack.parse("./", nil)
	pack.generate()
}
