package list

import (
	"strconv"
	"testing"
)

func TestLinkedList_AddLeft(t *testing.T) {
	list := NewLinkedList()
	list.AddLeft([]byte("hello"))
	list.AddLeft([]byte("world"))
	list.AddLeft([]byte("!"))

	if string(list.Right()) != "hello" {
		t.Fail()
	}
	if string(list.Left()) != "!" {
		t.Fail()
	}
	t.Log(string(list.Left()))
	t.Log(string(list.Right()))
}

func TestLinkedList_AddRight(t *testing.T) {
	list := NewLinkedList()
	list.AddRight([]byte("hello"))
	list.AddRight([]byte("world"))
	list.AddRight([]byte("!"))

	if string(list.Right()) != "!" {
		t.Fail()
	}
	if string(list.Left()) != "hello" {
		t.Fail()
	}
	t.Log(string(list.Left()))
	t.Log(string(list.Right()))
}

func TestLinkedList_Get(t *testing.T) {
	list := NewLinkedList()
	list.AddRight([]byte("v1"))
	list.AddRight([]byte("v2"))
	list.AddRight([]byte("v3"))
	list.AddRight([]byte("v4"))

	for i := 0; i < 4; i++ {
		if string(list.Get(i)) != string(list.Get(i-4)) {
			t.Fail()
		}
	}
}

func TestLinkedList_RemoveLeft(t *testing.T) {
	list := NewLinkedList()
	list.AddRight([]byte("v1"))
	list.AddRight([]byte("v2"))
	list.AddRight([]byte("v3"))

	if string(list.RemoveLeft()) != "v1" || string(list.RemoveLeft()) != "v2" || string(list.RemoveLeft()) != "v3" {
		t.Fail()
	}
	if list.Left() != nil || list.Right() != nil || list.Size() != 0 {
		t.Fail()
	}
}

func TestLinkedList_RemoveRight(t *testing.T) {
	list := NewLinkedList()
	list.AddLeft([]byte("v1"))
	list.AddLeft([]byte("v2"))
	list.AddLeft([]byte("v3"))

	if string(list.RemoveRight()) != "v1" || string(list.RemoveRight()) != "v2" || string(list.RemoveRight()) != "v3" {
		t.Fail()
	}
	if list.Left() != nil || list.Right() != nil || list.Size() != 0 {
		t.Fail()
	}
}

func TestLinkedList_LeftRange(t *testing.T) {
	list := NewLinkedList()
	list.AddRight([]byte("1"))
	list.AddRight([]byte("2"))
	list.AddRight([]byte("3"))
	list.AddRight([]byte("4"))
	list.AddRight([]byte("5"))
	list.AddRight([]byte("6"))

	arr := list.LeftRange(0, list.Size()-1)
	for i, value := range arr {
		if num, err := strconv.Atoi(string(value)); err != nil || num != i+1 {
			t.Fail()
		}
	}

	arr = list.LeftRange(5, 6)
	if len(arr) != 1 || string(arr[0]) != "6" {
		t.Fail()
	}

	arr = list.LeftRange(10, 100)
	if arr != nil {
		t.Fail()
	}
}
