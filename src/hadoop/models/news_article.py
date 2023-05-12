class NewsArticle:
    def __init__(self) -> None:
        self.title : str = ""
        self.url : str = ""
        self.date : str = ""
        self.authors : str = ""
        self.publisher : str = ""
        self.short_description : str = ""
        self.text : str = ""
        self.category : str = ""
        self.stock : str = ""
        self.country : str = ""
        
    @staticmethod
    def clean(word : str) -> str:
        word = word.replace('"', "")
        word = word.replace('\\', "")
        return word
    
    #@staticmethod
    #def clean_value(text : str) -> str:
    #    text = text.replace('"', "")
    #    text = text.replace("'", "")
    #    text = text.replace('\\', "")
    #    text = '"' + text + '"'
    #    return text

    @staticmethod
    def remove_comma(text : str) -> str:
        return text.strip(',')
    
    @staticmethod
    def get_article_id(obj : any) -> str:
        return str(hash(obj))
    
    @staticmethod
    def clean_and_split(line : str) -> tuple:
        line = line.strip()
        search_word = '": '
        if line.find(search_word) == -1:
            return line, ""
        res = line.split(search_word)
        value = res[1]
        if len(res) > 2:
            value = search_word.join(res[1:])
        key = NewsArticle.clean(res[0])
        #value = NewsArticle.clean_value(value)
        return key, value

    def match(self, key : str, value : str) -> str:
        value = NewsArticle.remove_comma(value)
        if key == "title":
            self.title = value
        if key == "url":
            self.url = value
        if key == "date":
            self.date = value
        if key == "authors":
            self.authors = value
        if key == "publisher":
            self.publisher = value
        if key == "short_description":
            self.short_description = value
        if key == "text":
            self.text = value
        if key == "category":
            self.category = value
        if key == "stock":
            self.stock = value
        if key == "country":
            self.country = value
        return value

    def to_tuple(self) -> tuple:
        return (self.title, self.url, self.date, self.authors, 
                self.publisher, self.short_description, self.text, 
                self.category, self.stock, self.country)

    def to_csv(self) -> str:
        return f"{self.title},{self.url},{self.date},{self.authors},{self.publisher},{self.short_description},{self.text},{self.category},{self.stock},{self.country}"
