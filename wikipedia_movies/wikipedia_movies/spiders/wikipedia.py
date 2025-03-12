import scrapy  
import requests  


class WikipediaSpider(scrapy.Spider):  
    name = "wikipedia"  
    allowed_domains = ["ru.wikipedia.org"]  
    # Начинаем обход с категории "Фильмы по алфавиту"  
    start_urls = [  
        "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"  
    ]  

    omdb_api_key = "87ac4b28"  

    def parse(self, response):  
        """  
        Парсим список ссылок на фильмы в категории.   
        Переходим на каждую страницу фильма, а также на следующую страницу категории.  
        """  
        # Сбор ссылок на статьи о фильмах  
        film_links = response.xpath('//div[@class="mw-category"]//a/@href').extract()  
        self.logger.info(f"Найдено {len(film_links)} ссылок на фильмы на странице {response.url}")  

        for link in film_links:  
            # Переходим на страницу фильма  
            yield response.follow(link, callback=self.parse_film)  

        # Переход на следующую страницу категории (если есть)  
        next_page = response.xpath('//a[contains(text(), "Следующая страница")]/@href').get()  
        if next_page:  
            self.logger.info(f"Переходим на следующую страницу категории: {next_page}")  
            yield response.follow(next_page, callback=self.parse)  

    def parse_film(self, response):  
        """  
        Парсим страницу конкретного фильма. Ищем инфобокс и пытаемся извлечь:  
        Название, жанр, режиссёр, страна, год. Параллельно – рейтинг IMDb.  
        """  
        # Название страницы (обычно – название фильма)  
        title = response.xpath('//h1[@id="firstHeading"]/text()').get()  

        # Инфобокс (если есть) обычно имеет класс "infobox"  
        info_box = response.xpath('//table[contains(@class, "infobox")]')  

        # Извлекаем жанр  
        genre = info_box.xpath('.//th[contains(text(), "Жанр")]/following-sibling::td//text()').get()  
        # Извлекаем режиссёра  
        director = info_box.xpath('.//th[contains(text(), "Режиссёр")]/following-sibling::td//text()').get()  
        # Извлекаем страну  
        country = info_box.xpath('.//th[contains(text(), "Страна")]/following-sibling::td//text()').get()  
        # Извлекаем год  
        year = info_box.xpath('.//th[contains(text(), "Год")]/following-sibling::td//text()').get()  

        # Объединяем данные в словарь  
        movie_data = {  
            "title": title.strip() if title else None,  
            "genre": genre.strip() if genre else None,  
            "director": director.strip() if director else None,  
            "country": country.strip() if country else None,  
            "year": year.strip() if year else None,  
            "imdb_rating": None  # Будем заполнять ниже  
        }  

        # Логируем для отладки  
        self.logger.info(f"Собранные данные (без рейтинга) для {title}: {movie_data}")  

        # Если у нас есть название фильма, пытаемся получить рейтинг IMDb  
        if title:  
            movie_data["imdb_rating"] = self.get_imdb_rating(title)  

        yield movie_data  

    def get_imdb_rating(self, title):  
        """  
        Выполняем запрос к OMDb API, чтобы получить рейтинг IMDb по названию фильма.  
        """  
        omdb_url = "http://www.omdbapi.com/"  
        params = {  
            "t": title,  
            "apikey": self.omdb_api_key  
        }  
        try:  
            response = requests.get(omdb_url, params=params)  
            if response.status_code == 200:  
                data = response.json()  
                imdb_rating = data.get("imdbRating")  
                self.logger.info(f"Рейтинг IMDb для '{title}': {imdb_rating}")  
                return imdb_rating  
            else:  
                self.logger.warning(f"OMDb API вернул код {response.status_code} для '{title}'")  
                return None  
        except Exception as e:  
            self.logger.error(f"Ошибка при запросе рейтинга для '{title}': {e}")  
            return None  