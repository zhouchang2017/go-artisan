package main

import (
	"fmt"
)

func main() {
	cond := map[string]interface{}{
		"_limit": []uint{0, 15},
	}
	limit := cond["_limit"]
	delete(cond, "_limit")
	fmt.Printf("%v\n", cond)

	cond["_limit"] = limit
	fmt.Printf("%v\n", cond)
}
