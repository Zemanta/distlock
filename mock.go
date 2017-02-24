package distlock

type MockReleaser struct {
}

func (mr *MockReleaser) Release() error {
	return nil
}

type MockLocker struct {
}

func (ml *MockLocker) Lock(string) (Releaser, error) {
	return &MockReleaser{}, nil
}

func (ml *MockLocker) LockWait(string) (Releaser, error) {
	return &MockReleaser{}, nil
}
