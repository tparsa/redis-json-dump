class ArgsMock:
    def add(self, name, value):
        setattr(self, name, value)
