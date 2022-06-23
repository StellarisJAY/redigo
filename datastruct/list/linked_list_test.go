package list

import "testing"

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
