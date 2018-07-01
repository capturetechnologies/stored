package packed

import (
	"fmt"
)

func asset(name string, err error) {
	if err == nil {
		fmt.Println("Success «" + name + "»")
	} else {
		fmt.Println("Fail «"+name+"»:", err)
	}
}

func testSimple() error {
	intPacker := New(int(0))
	dataBytes, err := intPacker.Encode(4)
	if err != nil {
		return err
	}

	intV := 0
	err = intPacker.Decode(dataBytes, &intV)
	if err != nil {
		return err
	}
	if intV != 4 {
		return fmt.Errorf("incorrect int: %d", intV)
	}

	sliceStringPacker := New([]string{})
	dataBytes, err = sliceStringPacker.Encode([]string{"one", "two", "three"})
	if err != nil {
		return err
	}

	resSliceString := []string{}
	err = sliceStringPacker.Decode(dataBytes, &resSliceString)
	if err != nil {
		return err
	}
	if len(resSliceString) != 3 || resSliceString[0] != "one" || resSliceString[1] != "two" || resSliceString[2] != "three" {
		return fmt.Errorf("incorrect string slice: %v", resSliceString)
	}

	return nil
}

func testSetGet() error {
	type user struct {
		Name  string
		ID    int
		Score int64
		Bio   []byte
	}
	packer := New(user{})
	u := user{
		ID:    2,
		Name:  "John",
		Score: 157893,
		Bio:   []byte("Test bio"),
	}
	dataBytes, err := packer.Encode(u)
	if err != nil {
		return err
	}

	fetch := user{}
	err = packer.Decode(dataBytes, &fetch)
	if err != nil {
		return err
	}
	if fetch.ID != 2 || fetch.Name != "John" || fetch.Score != 157893 {
		return fmt.Errorf("incorrect data id: %v, name: %v, score: %v", fetch.ID, fetch.Name, fetch.Score)
	}
	if string(fetch.Bio) != "Test bio" {
		return fmt.Errorf("incorrect bio: %s", fetch.Bio)
	}
	return nil
}

func Test() {
	asset("Simple", testSimple())
	asset("SetGet", testSetGet())
}
