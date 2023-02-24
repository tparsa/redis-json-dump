class FileMock:
    def __init__(self):
        self.content = ""

    def __iter__(self):
        lines = self.content.split("\n")
        return (line for line in lines)

    def write(self, text):
        self.content += text

    def get_content(self):
        return self.content

    