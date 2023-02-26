class RetentionBucket:
    def __init__(self, count, bucker):
        self.__count = count
        self.__bucker = bucker
        self.__last = -1
        self.count = count
        self.bucker = bucker
        self.last = -1

    def reset(self):
        self.count = self.__count
        self.bucker = self.__bucker
        self.last = self.__last
