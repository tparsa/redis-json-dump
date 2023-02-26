class ArgsMock:
    def __init__(self):
        self.__argv = []
    
    def add(self, name, value, single_dashed=False):
        setattr(self, name, value)
        name = name.replace("_", "-")
        if not single_dashed:
            self.__argv.append("--" + name)
        else:
            self.__argv.append("-" + name)
        if value not in [True, False]:
            self.__argv.append(value)

    def argv(self):
        return self.__argv