package stored

type Storage interface {
	GetField(string) interface{}
}
