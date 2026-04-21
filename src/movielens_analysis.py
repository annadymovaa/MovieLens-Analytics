#!/usr/bin/env python3
#Разрешённые импорты: os, sys, urllib, requests, beautifulsoup, json, pytest, collections, functools, datetime, re.
import requests
import sys
import re
from datetime import datetime
from collections import Counter
import os
import pytest

class Links:
#     """
#     Analyzing data from links.csv
#     """
    def __init__(self, path='../datasets/ml-latest-small-1000/links_1000.csv',
                 movies_path='../datasets/ml-latest-small-1000/movies_1000.csv'):
        # Проверка links.csv
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            raise FileNotFoundError(f"Файл не найден или пустой: {path}")
        
        self.path = path
        self.links = {}       # {movie_id: imdb_id}
        self.movies = {}      # {movie_id: title} ← загружается из movies.csv
        self.imdb_info = []   # [{movie_id, Title, Director, ...}, ...]
        
        # 1️⃣ Загрузка данных
        self._load_links()
        self._load_movies(movies_path)  # ← Слияние в конструкторе

    def _load_links(self):
        """Загружает links.csv в self.links"""
        with open(self.path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            try:
                parts = line.split(',')
                if len(parts) < 2:
                    continue
                mid = int(parts[0].strip().strip('"'))
                iid_str = parts[1].strip().strip('"')
                self.links[mid] = int(iid_str) if iid_str and iid_str.lower() not in ('0', 'n/a', 'null', 'nan', '') else None
            except Exception:
                continue

    def _load_movies(self, movies_path):
        """2️⃣ Загружает movies.csv в self.movies = {movie_id: title}"""
        if not os.path.exists(movies_path):
            return
        with open(movies_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            try:
                # Разбиваем только на 3 части: movieId, title, genres (title может содержать запятые)
                parts = line.split(',', 2)
                if len(parts) < 2:
                    continue
                mid = int(parts[0].strip().strip('"'))
                title = parts[1].strip().strip('"')
                self.movies[mid] = title  # ← Ключевое: сохраняем название по movie_id
            except Exception:
                continue

    def _ensure_data_loaded(self):
        if not self.imdb_info:
            raise ValueError("Нет данных в imdb_info. Сначала вызовите get_imdb().")

    def get_imdb(self, list_of_movies, list_of_fields):
        """
        Возвращает список списков [movieId, field1, field2, ...]
        Сортировка по movieId по убыванию.
        """
        if not isinstance(list_of_movies, list):
            raise TypeError("list_of_movies должен быть списком")
        if not isinstance(list_of_fields, list):
            raise TypeError("list_of_fields должен быть списком")

        GRAPHQL_URL = "https://api.graphql.imdb.com/"
        FIELD_MAP = {
            "Director": "directors",
            "Budget": "productionBudget", 
            "Cumulative Worldwide Gross": "lifetimeGross",
            "Runtime": "runtime",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.imdb.com",
            "Referer": "https://www.imdb.com/",
        }

        results = []

        for movie_id in list_of_movies:
            imdb_numeric = self.links.get(movie_id)
            if imdb_numeric is None:
                continue

            imdb_id = f"tt{imdb_numeric:07d}"
            query_parts = ["titleText { text }"]
            for field in list_of_fields:
                gql_field = FIELD_MAP.get(field)
                if gql_field == "directors":
                    query_parts.append('principalCredits(filter: {categories: ["director"]}) { credits { name { nameText { text } } } }')
                elif gql_field == "productionBudget":
                    query_parts.append("productionBudget { budget { amount currency } }")
                elif gql_field == "lifetimeGross":
                    query_parts.append("lifetimeGross(boxOfficeArea: WORLDWIDE) { total { amount currency } }")
                elif gql_field == "runtime":
                    query_parts.append("runtime { seconds }")

            query = f"""
            query GetMovieData($id: ID!) {{
              title(id: $id) {{
                {" ".join(query_parts)}
              }}
            }}
            """
            payload = {"query": query, "variables": {"id": imdb_id}}

            try:
                response = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=15)
                if response.status_code != 200:
                    continue
                    
                result = response.json()
                data = (result.get("data") or {}).get("title") or {}
                if not data:
                    continue

                # 3️⃣ Приоритет: название из API, fallback — из movies.csv
                title = (data.get("titleText") or {}).get("text") or self.movies.get(movie_id, "Unknown Title")
                
                movie_dict = {"movie_id": movie_id, "Title": title}
                for field in list_of_fields:
                    movie_dict[field] = self._extract_graphql_value(data, field)
                self.imdb_info.append(movie_dict)

                row = [movie_id]
                for field in list_of_fields:
                    row.append(movie_dict[field])
                results.append(row)

            # 4️⃣ Обработка ошибок через исключения
            except requests.exceptions.RequestException:
                continue
            except Exception:
                continue
        imdb_info = sorted(results, key=lambda x: str(x[0]), reverse=True)
        return imdb_info


    def _extract_graphql_value(self, data, field):
        """Извлекает значение поля из GraphQL ответа IMDB (исправленная версия)."""
        try:
            if field == "Director":
                directors = []
                for section in (data.get("principalCredits") or []):
                    cat = section.get("category") or {}
                    
                    # 🔹 Если category есть — проверяем, что это режиссёры
                    # 🔹 Если category НЕТ — берём все имена (безопасный фолбэк)
                    if cat:
                        cat_id = str(cat.get("id", "")).lower()
                        cat_text = str(cat.get("text", "")).lower()
                        if "director" not in cat_id and "director" not in cat_text:
                            continue  # Пропускаем, если это не режиссёры
                    
                    # Извлекаем имена из credits
                    for credit in (section.get("credits") or []):
                        name_obj = credit.get("name") or {}
                        name_text = name_obj.get("nameText") or {}
                        name = name_text.get("text")
                        if name and isinstance(name, str) and name.strip():
                            directors.append(name.strip())
                
                return ", ".join(directors) if directors else None
                
            elif field == "Budget":
                budget_obj = (data.get("productionBudget") or {}).get("budget") or {}
                amount = budget_obj.get("amount")
                return f"${int(amount)}" if amount is not None else None
                
            elif field == "Cumulative Worldwide Gross":
                gross_obj = (data.get("lifetimeGross") or {}).get("total") or {}
                amount = gross_obj.get("amount")
                return f"${int(amount)}" if amount is not None else None
                
            elif field == "Runtime":
                runtime_obj = data.get("runtime") or {}
                seconds = runtime_obj.get("seconds")
                if isinstance(seconds, (int, float)) and seconds > 0:
                    return f"{int(seconds // 60)} min"
                return None
                
            return None
        except Exception:
            return None  # Безопасный возврат при любой ошибке

    def top_directors(self, n=5):
        """ТОП режиссёров по количеству фильмов (с визуализацией)."""
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
            
        self._ensure_data_loaded()
        
        counter = Counter()
        for m in self.imdb_info:
            director = m.get("Director")
            if director:
                counter[director] += 1
                
        # Сортировка: кол-во (DESC), имя (DESC)
        sorted_directors = sorted(counter.items(), key=lambda x: (x[1], x[0]), reverse=True)
        directors_dict = dict(sorted_directors)

        print(f"🎬 ТОП-{n} РЕЖИССЁРОВ:")
        for place, (director, count) in enumerate(sorted_directors[:n], 1):
            print(f"{place}. {director}")
            
        return directors_dict  # ← возвращаем dict для аналитики/графиков
        
    def most_expensive(self, n=5):
        """ТОП самых дорогих фильмов по бюджету (с визуализацией)."""
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
            
        self._ensure_data_loaded()
        
        budgets = {}
        for m in self.imdb_info:
            title = m.get("Title")
            raw = m.get("Budget")
            if title and raw and "$" in str(raw):
                digits = ''.join(c for c in str(raw) if c.isdigit())
                if digits:
                    budgets[title] = int(digits)
        
        # Сортировка по бюджету (DESC)
        sorted_budgets = sorted(budgets.items(), key=lambda x: x[1], reverse=True)[:n]
        budgets_dict = dict(sorted_budgets)

        # 🎨 Печать в стиле top_directors: заголовок + бар-чарт
        print(f"💰 ТОП-{n} САМЫХ ДОРОГИХ ФИЛЬМОВ:")
        
        # Масштаб бара: 1 █ = $10 млн (подберите под ваши данные)
        scale = 10_000_000  
        max_budget = sorted_budgets[0][1] if sorted_budgets else 1
        
        for place, (title, budget) in enumerate(sorted_budgets, 1):
            # Бар пропорционален бюджету относительно самого дорогого
            bar_len = max(1, int((budget / max_budget) * 20))  # макс. 20 символов
            bar = "█" * bar_len
            # Форматируем бюджет: $100,000,000
            budget_fmt = f"${budget:,}"
            print(f"{place}. {title}: {bar} {budget_fmt}")
            
        return budgets_dict  # ← возвращаем dict для аналитики/графиков
        
    def most_profitable(self, n=5):
        """ТОП самых прибыльных фильмов (Gross - Budget) с визуализацией."""
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
            
        self._ensure_data_loaded()
        
        profits = {}
        for m in self.imdb_info:
            title = m.get("Title")
            raw_budget = m.get("Budget")
            raw_gross = m.get("Cumulative Worldwide Gross")
            
            if title and raw_budget and raw_gross and "$" in str(raw_budget) and "$" in str(raw_gross):
                b = int(''.join(c for c in str(raw_budget) if c.isdigit()))
                g = int(''.join(c for c in str(raw_gross) if c.isdigit()))
                if b == 0:
                    continue
                profits[title] = g - b
                
        # Сортировка по прибыли (DESC)
        sorted_profits = sorted(profits.items(), key=lambda x: x[1], reverse=True)[:n]
        profits_dict = dict(sorted_profits)

        # 🎨 Печать в стиле top_directors: заголовок + бар-чарт
        print(f"🚀 ТОП-{n} САМЫХ ПРИБЫЛЬНЫХ ФИЛЬМОВ:")
        
        # Масштаб: относительный бар относительно макс. прибыли
        max_profit = sorted_profits[0][1] if sorted_profits else 1
        
        for place, (title, profit) in enumerate(sorted_profits, 1):
            # 📊 Бар: зелёный для прибыли, красный для убытка
            if profit >= 0:
                bar_len = max(1, int((profit / max_profit) * 20))
                bar = "█" * bar_len
                sign = "+"
            else:
                # Для убытков: обратный бар (левее)
                bar_len = max(1, int((abs(profit) / abs(max_profit)) * 20))
                bar = "▏" * bar_len  # или "░" для визуального отличия
                sign = "−"
                
            # Форматируем сумму: +$100,000,000
            profit_fmt = f"{sign}${abs(profit):,}"
            print(f"{place}. {title}: {bar} {profit_fmt}")
            
        return profits_dict  # ← возвращаем dict для аналитики/графиков
        
    def longest(self, n=10):
        """ТОП самых длинных фильмов по хронометражу (с визуализацией)."""
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
            
        self._ensure_data_loaded()
        
        runtimes = {}
        for m in self.imdb_info:
            title = m.get("Title")
            r = m.get("Runtime")
            if title and r:
                match = re.search(r'(\d+)\s*min', str(r), re.IGNORECASE)
                if match:
                    minutes = int(match.group(1))
                    if minutes > 0:
                        runtimes[title] = minutes
                        
        # Сортировка по длительности (DESC)
        sorted_runtimes = sorted(runtimes.items(), key=lambda x: x[1], reverse=True)[:n]
        runtimes_dict = dict(sorted_runtimes)

        # 🎨 Печать: заголовок + бар-чарт
        print(f"⏱️ ТОП-{n} САМЫХ ДЛИННЫХ ФИЛЬМОВ:")
        
        max_runtime = sorted_runtimes[0][1] if sorted_runtimes else 1
        
        for place, (title, minutes) in enumerate(sorted_runtimes, 1):
            # Бар пропорционален длительности (макс. 20 символов)
            bar_len = max(1, int((minutes / max_runtime) * 20))
            bar = "█" * bar_len
            # Форматируем время: 2ч 45мин
            hours, mins = divmod(minutes, 60)
            time_fmt = f"{hours}ч {mins}мин" if hours > 0 else f"{mins}мин"
            print(f"{place}. {title}: {bar} {time_fmt}")
            
        return runtimes_dict  # ← возвращаем dict для аналитики
        
    def top_cost_per_minute(self, n=10):
        """ТОП фильмов по стоимости минуты съёмок (с визуализацией)."""
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
            
        self._ensure_data_loaded()
        
        costs = {}
        for m in self.imdb_info:
            title = m.get("Title")
            r = m.get("Runtime")
            b = m.get("Budget")
            if not (title and r and b):
                continue
            try:
                budget_val = b if isinstance(b, (int, float)) else int(''.join(c for c in str(b) if c.isdigit()) or 0)
                if budget_val == 0:
                    continue
                match = re.search(r'(\d+)\s*min', str(r), re.IGNORECASE)
                if not match:
                    continue
                total_min = int(match.group(1))
                if total_min == 0:
                    continue
                cost = round(budget_val / total_min, 2)
                costs[title] = cost
            except (ValueError, IndexError):
                continue
                
        # Сортировка по стоимости/мин (DESC)
        sorted_costs = sorted(costs.items(), key=lambda x: x[1], reverse=True)[:n]
        costs_dict = dict(sorted_costs)

        # 🎨 Печать: заголовок + бар-чарт
        print(f"💸 ТОП-{n} ПО СТОИМОСТИ МИНУТЫ СЪЁМОК:")
        
        max_cost = sorted_costs[0][1] if sorted_costs else 1
        
        for place, (title, cost_per_min) in enumerate(sorted_costs, 1):
            # Бар пропорционален стоимости (макс. 20 символов)
            bar_len = max(1, int((cost_per_min / max_cost) * 20))
            bar = "█" * bar_len
            # Форматируем: $1,234,567.89
            cost_fmt = f"${cost_per_min:,.2f}"
            print(f"{place}. {title}: {bar} {cost_fmt}/мин")
            
        return costs_dict  # ← возвращаем dict для аналитики


class Movies:
    def __init__(self, path_to_the_file='../datasets/ml-latest-small-1000/movies_1000.csv'):
        self.path = os.path.normpath(path_to_the_file)
        self.movies_data = []

        if not os.path.exists(self.path):
            raise FileNotFoundError(f"Файл {self.path} не найден.")

        try:
            with open(self.path, mode='r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
                if not lines:
                    return

                pattern = re.compile(r'^(\d+),("(?:[^"]|"")*"|[^,]+),(.+)$')            # Регулярка для обработки: movieId, "Title, with, commas (2000)", Genres  Позволяет корректно считывать названия в кавычках.
                
                for line in lines[1:]:                                                  # Пропуск заголовка
                    line = line.strip()
                    if not line:
                        continue
                    
                    match = pattern.match(line)
                    if match:
                        mid, title, genres = match.groups()
                        if title.startswith('"') and title.endswith('"'):               # Очистка названия от лишних кавычек
                            title = title[1:-1].replace('""', '"')
                        
                        self.movies_data.append({
                            'movieId': mid,
                            'title': title,
                            'genres': genres
                        })
        except Exception as e:
            raise RuntimeError(f"Ошибка при обработке файла: {e}")

    def dist_by_release(self):
        """Распределение по годам выпуска (сортировка по количеству DESC)."""
        years = []
        for row in self.movies_data:
            match = re.search(r'\((\d{4})\)$', row['title'].strip())
            if match:
                years.append(match.group(1))
        counts = Counter(years)                                                             # Сортировка: кол-во (DESC), год (DESC)
        sorted_years = sorted(counts.items(), key=lambda x: (x[1], x[0]), reverse=True)     
        year_dist = dict(sorted_years)

        print("📅 КОЛИЧЕСТВО ФИЛЬМОВ ПО ГОДАМ (1987-1995):")
        for year, count in list(year_dist.items())[:10]:
            bar = "█" * (count // 5)
            print(f"{year}: {bar} {count}")
        return year_dist

    def dist_by_genres(self):
        """Распределение по жанрам (сортировка по количеству DESC)."""
        all_genres = []
        for row in self.movies_data:
            genres_str = row.get('genres', '')
            if genres_str and genres_str != '(no genres listed)':
                all_genres.extend(genres_str.split('|'))
        counts = Counter(all_genres)                                                        # Сортировка: кол-во (DESC), название (DESC)
        sorted_genres = sorted(counts.items(), key=lambda x: (x[1], x[0]), reverse=True)
        genre_dist = dict(sorted_genres)

        print("🎭 ТОП-10 ЖАНРОВ:")
        for i, (genre, count) in enumerate(list(genre_dist.items())[:10], 1):
            print(f"{i}. {genre}: {count} фильмов")
        return genre_dist

    def most_genres(self, n):
        """Топ-n фильмов по количеству жанров (сортировка по кол-ву DESC)."""
        if not isinstance(n, int) or n <= 0:
            return {}
        
        movie_counts = []
        for row in self.movies_data:
            g_list = row['genres'].split('|')
            count = 0 if row['genres'] == '(no genres listed)' else len(g_list)
            movie_counts.append((row['title'], count))
            
        sorted_items = sorted(movie_counts, key=lambda x: (-x[1], x[0]))                    # Сортировка: кол-во (DESC), название (ASC)
        multi_genre = dict(sorted_items[:n])
        print("🎭 САМЫЕ МНОГОЖАНРОВЫЕ ФИЛЬМЫ:")
        for title, count in multi_genre.items():
            print(f"• {title} — {count} жанров")
        return multi_genre

    def get_top_franchises(self, n=5):
        """Бонус: Поиск франшиз."""
        titles = [re.sub(r'\s*\(\d{4}\)$', '', row['title']).strip() for row in self.movies_data]
        counts = Counter(titles)
        franchises = {k: v for k, v in counts.items() if v > 1}
        top_franchises = dict(sorted(franchises.items(), key=lambda x: x[1], reverse=True)[:n])
        
        place = 1
        for title, num in top_franchises.items():
            print(f'{place}. {title} ({num} фильма)')
            place += 1
        return top_franchises

    def get_most_productive_year_by_genre(self, genre='Comedy'):
        """Бонус: Пиковый год для жанра."""
        years = []
        for row in self.movies_data:
            if genre in row.get('genres', '').split('|'):
                match = re.search(r'\((\d{4})\)$', row['title'].strip())
                if match:
                    years.append(match.group(1))
        if not years:
            return None
        peak_year_comedy = Counter(years).most_common(1)
        
        (year, num) = peak_year_comedy[0]
        print(f'Самый "горячий" год для жанра {genre} - {year} ({num} фильмов)')
        
        return peak_year_comedy

class Ratings:
    """
    Analyzing data from ratings.csv
    """
    def __init__(self, path='../datasets/ml-latest-small-1000/ratings_1000.csv'):
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            raise FileNotFoundError(f"Файл не найден или пустой: {path}")
        
        self.filepath = path
        self.get_content()
        self.users = self.Users(self)

    def get_content(self):
        with open(self.filepath, 'r', encoding='utf-8') as file:
            content = file.readlines()
        movies = Movies()
        self.content = list()
        for i in content[1:]:
            line = i.split(',')
            line_dict = {
                'userId' : line[0].strip(),
                'movieId' : line[1].strip(),
                'rating' : float(line[2].strip()),
                'timestamp' : int(line[3].strip()),
                'title' : None
            }
            
            for movie in movies.movies_data:
                if str(line_dict['movieId']) == str(movie['movieId']):
                    line_dict['title'] = movie['title']
                    break
            self.content.append(line_dict)


    class Movies:  
        def __init__(self, outer_instance):
            #сохраняем ссылку на внешний объект
            self.outer = outer_instance  

        def dist_by_year(self, n=10):
            #{year : count} по ключам по возрастанию

            years = []
            for line in self.outer.content:
                timestamp = line['timestamp']
                m_year = datetime.fromtimestamp(timestamp).year
                years.append(m_year)
            count_by_year = Counter(years)
            ratings_by_year = dict(sorted(count_by_year.items()))

            print("📅 КОЛИЧЕСТВО ОЦЕНОК ФИЛЬМОВ ПО ГОДАМ (1996 - 2015):")
            for year, count in list(ratings_by_year.items())[:n]:
                bar = "█" * (count // 5)
                print(f"{year}: {bar} {count}")
            return ratings_by_year
        
        def dist_by_rating(self):
            #{rating : count} по рейтингу по возрастанию

            ratings = []
            for line in self.outer.content:
                rating = line['rating']
                ratings.append(rating)
            count_by_rating = Counter(ratings)
            ratings_distribution = dict(sorted(count_by_rating.items()))
            print("Распределение по количеству оценок:")
            for rating, count in ratings_distribution.items():
                print(f"⭐ {rating} — {count} оценок")
            return ratings_distribution
                
        def dict_of_mov_rats(self):
            self.mov_rating = dict()
            for line in self.outer.content:
                key = (line['movieId'], line['title'])
                if key not in self.mov_rating:
                    self.mov_rating[key] = list()
                self.mov_rating[key].append(line['rating'])
        
        def top_by_num_of_ratings(self, n=10):
            #{movie_title : num of ratings} по номерам по убыванию
            self.dict_of_mov_rats()
            mov_rating = list()
            for (movieId, title), ratings in self.mov_rating.items():
                if title is not None:
                    mov_rating.append((title, len(ratings)))

            movies = list(sorted(mov_rating, key=lambda item: item[1], reverse=True))
            top_movies = dict(movies[:n])

            print("🏆 ТОП-10 ФИЛЬМОВ ПО КОЛИЧЕСТВУ ОЦЕНОК:")
            for title, metric in top_movies.items():
                print(f"• {title} - {metric} оценки")
            return top_movies
        
        def top_by_ratings(self, n=10, metric='average'):
            self.dict_of_mov_rats()
            movie_metrics = [] 

            for (m_id, title), list_of_rats in self.mov_rating.items():
                if title is None: continue  # Пропускаем фильмы без названий
                
                num = len(list_of_rats)
                if metric == 'average':
                    metric_v = sum(list_of_rats) / num
                elif metric == 'median':
                    sorted_rating = sorted(list_of_rats)
                    mid = num // 2
                    if num % 2 == 1:
                        metric_v = sorted_rating[mid]
                    else:
                        metric_v = (sorted_rating[mid - 1] + sorted_rating[mid]) / 2
                else:
                    raise Exception('The metric is wrong! Choose between average and median')
                
                movie_metrics.append((title, round(metric_v, 2)))
            
            movie_metrics.sort(key=lambda x: x[1], reverse=True)
            top_movies = dict(movie_metrics[:n])

            print(f"🏆 ТОП-10 ФИЛЬМОВ ПО {metric.upper()}:")
            place = 1
            for title, metric_v in top_movies.items():
                print(f"{place}. {title} ({metric_v} ⭐)")
                place += 1
            return top_movies

        def top_controversial(self, n=10):
            self.dict_of_mov_rats()
            movie_metrics = []

            for (m_id, title), list_of_rats in self.mov_rating.items():
                if title is None: continue
                
                rat_count = len(list_of_rats)
                if rat_count < 2: # Дисперсия одного числа всегда 0
                    variance = 0
                else:
                    mean = sum(list_of_rats) / rat_count
                    cumulator = sum((rating - mean) ** 2 for rating in list_of_rats)
                    variance = cumulator / rat_count
                    
                movie_metrics.append((title, round(variance, 2)))

            movie_metrics.sort(key=lambda x: x[1], reverse=True)
            controversial_movies = dict(movie_metrics[:n])

            place = 1
            for title, variance in controversial_movies.items():
                print(f'{place} место : {title} (дисперсия оценок {variance})')
                place += 1

            return controversial_movies

        
        def top_by_max_ratings(self, n=1):
            self.dict_of_mov_rats()
            max_rating_count = []

            for (m_id, title), ratings in self.mov_rating.items():
                if title is None: continue
                
                count_max = ratings.count(5.0)
                if count_max > 0:
                    max_rating_count.append((title, count_max))

            max_rating_count.sort(key=lambda x: x[1], reverse=True)
            top_favorite_movie = dict(max_rating_count[:n])

            place = 1
            for title, count in top_favorite_movie.items():
                print(f'{place} : {title} (кол-во отличных оценок {count})')
                place += 1
            return top_favorite_movie


    class Users(Movies):
        """
        In this class, three methods should work. 
        The 1st returns the distribution of users by the number of ratings made by them.
        The 2nd returns the distribution of users by average or median ratings made by them.
        The 3rd returns top-n users with the biggest variance of their ratings.
     Inherit from the class Movies. Several methods are similar to the methods from it.
        """
    
        def dist_by_activity(self):
            #{user : count} по кол-ву оценок по убыванию

            users = []
            for line in self.outer.content:
                user = line['userId']
                users.append(user)
            count_by_ratings = Counter(users)
            ratings_by_user = dict(sorted(count_by_ratings.items(), key=lambda item: item[1], reverse=True))
            place = 1
            for user, ratings in ratings_by_user.items():
                print(f'{place} место : user {user} ({ratings} оценок)')
                place += 1
            return ratings_by_user
        
        def dict_of_user_rats(self):
            self.user_id_rat = dict()
            for line in self.outer.content:
                user_id = line['userId'] 
                rating = line['rating']  
                if user_id not in self.user_id_rat:
                    self.user_id_rat[user_id] = list()
                self.user_id_rat[user_id].append(rating)
        
        def dist_by_metric(self, metric='average'):
            self.dict_of_user_rats()

            user_id_metrics = dict()
            for user in self.user_id_rat:
                list_of_rats = self.user_id_rat[user]
                if metric == 'average':
                    num = len(list_of_rats)
                    if num != 1:
                        metric_v = sum(list_of_rats) / len(list_of_rats)
                    else:
                        metric_v = list_of_rats[0]
                elif metric == 'median':
                    num = len(list_of_rats)
                    if num != 1:
                        sorted_rating = sorted(list_of_rats)
                        mid = num // 2
                        if num % 2 == 1:
                            metric_v = sorted_rating[mid]
                        else:
                            metric_v = (sorted_rating[mid - 1] + sorted_rating[mid]) / 2
                    else:
                        metric_v = list_of_rats[0]
                else:
                    raise Exception('The metric is wrong! Choose between average and median')
            
                user_id_metrics[user] = round(metric_v, 2)
            
            user_ids = dict(sorted(user_id_metrics.items(), key=lambda item: item[1], reverse=True))
            place = 1
            print((f'\nТоп пользователей по {metric.upper()}:'))
            for user, metric_v in user_ids.items():
                print(f'{place} место : user {user} ({metric_v} ⭐)')
                place += 1
            return user_ids

        def top_controversial_users(self, n=3):
            self.dict_of_user_rats()
            user_id_metrics = dict()
            for movie in self.user_id_rat:
                list_of_rats = self.user_id_rat[movie]
                variance = 0
                rat_count = len(list_of_rats)
                if rat_count != 1:
                    cumulator = 0
                    mean = sum(list_of_rats) / rat_count
                    for rating in list_of_rats:
                        cumulator += (rating - mean) ** 2
                    variance = cumulator / rat_count
                user_id_metrics[movie] = round(variance, 2)

            user_ids = dict(sorted(user_id_metrics.items(), key=lambda item: item[1], reverse=True))
            top_user_ids = dict(list(user_ids.items())[:n])
            place = 1
            for user, variance in top_user_ids.items():
                print(f'{place} место : user {user} (дисперсия оценок {variance})')
                place += 1
            return top_user_ids


class Tags:
    """
    Analyzing data from tags.csv
    """
    def __init__(self, path='../datasets/ml-latest-small-1000/tags_1000.csv'):
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            raise FileNotFoundError(f"Файл не найден или пустой: {path}")
        
        self.path = path
        self.tags = []
        
        with open(self.path, 'r', encoding='utf-8') as file:
            next(file)  # пропускаем заголовок
            for line in file:
                parts = line.strip().split(',', 3)
                if len(parts) < 4:
                    continue
                
                try:
                    user_id = int(parts[0])
                    movie_id = int(parts[1])
                    tag = parts[2]
                    timestamp = int(parts[3])
                    self.tags.append({
                        'userId': user_id, 
                        'movieId': movie_id, 
                        'tag': tag, 
                        'timestamp': timestamp
                    })
                except ValueError:
                    #Пропускаем строки, где числа не парсятся, но продолжаем чтение файла
                    continue
    
    def most_words(self, n=5):
        """
        The method returns top-n tags with most words inside. It is a dict 
        where the keys are tags and the values are the number of words inside the tag.
        Drop the duplicates. Sort it by numbers descendingly.
        """
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
        
        #Убираем дубликаты, приводим к нижнему регистру и очищаем пробелы
        unique_tags = {i['tag'].strip().lower() for i in self.tags}
        
        #Считаем количество слов в каждом уникальном теге
        tag_counts = {tag: len(tag.split()) for tag in unique_tags}
        
        #Сортируем по количеству слов (по убыванию) и берём top-n
        big_tags = dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:n])

        print(f'\nТОП-{n} ТЕГОВ ПО КОЛИЧЕСТВУ СЛОВ:')
        place = 1
        for tags, num in big_tags.items():
            print(f'{place}. {tags} : {num} слов')
            place +=1
        return big_tags

    def longest(self, n=5):
        """
        The method returns top-n longest tags in terms of the number of characters.
        It is a list of the tags. Drop the duplicates. Sort it by numbers descendingly.
        """
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
            
        unique_tags = {tag['tag'].strip().lower() for tag in self.tags}
        big_tags = sorted(unique_tags, key=len, reverse=True)[:n]

        print(f'\nТОП-{n} ТЕГОВ ПО КОЛИЧЕСТВУ СИМВОЛОВ:')
        place = 1
        for tag in big_tags:
            print(f'{place}. {tag} : {len(tag)} символов')
            place +=1
        return big_tags

    def most_words_and_longest(self, n=10):

        """
        The method returns the intersection between top-n tags with most words inside and 
        top-n longest tags in terms of the number of characters.
        Drop the duplicates. It is a list of the tags.
        """
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
        
        most_words_tags = set(self.most_words(n).keys())
        longest_tags = set(self.longest(n))
        big_tags = sorted(most_words_tags & longest_tags)

        print(f'\nСАМЫЕ БОЛЬШИЕ ТЕГИ (по словам И символам):')
        place = 1
        for tag in big_tags:
            print(f'{place}. {tag} : {len(tag)} символов')
            place +=1        

        return big_tags
        
    def most_popular(self, n=5):
        """
        The method returns the most popular tags. 
        It is a dict where the keys are tags and the values are the counts.
        Drop the duplicates. Sort it by counts descendingly.
        """
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"Неверное значение аргумента: {n}")
            
        tag_list = [i['tag'].strip().lower() for i in self.tags]
        counter = Counter(tag_list)
        popular_tags = dict(counter.most_common(n))

        print(f'ТОП-{n} САМЫХ ПОПУЛЯРНЫХ ТЕГОВ:')
        place = 1
        for tag, count in popular_tags.items():
            print(f'{place}. {tag} : использован {count} раз')
            place +=1  

        return popular_tags
        
    def tags_with(self, word):
        """
        The method returns all unique tags that include the word given as the argument.
        Drop the duplicates. It is a list of the tags. Sort it by tag names alphabetically.
        """
        if not isinstance(word, str) or not word.strip():
            raise ValueError(f"Некорректное слово для поиска: {word}")
            
        word = word.strip().lower()
        tags_with_word = sorted({
            tag['tag'].strip().lower() 
            for tag in self.tags 
            if word in tag['tag'].strip().lower()
        })

        print(f'ТЕГИ СО СЛОВОМ {word}:')
        for tag in tags_with_word:
            print(f'- {tag}')
        
        return tags_with_word
    
    def dist_by_year(self):
        """
        The method returns the distribution of tags by year.
        It is a dict where the keys are years and the values are the counts.
        Sort it by counts descendingly.
        """
        years = []
        for tag in self.tags:
            ts = tag.get('timestamp')
            if ts:
                try:
                    years.append(datetime.fromtimestamp(ts).year)
                except (OSError, ValueError, OverflowError):
                    continue
                    
        year_distribution = dict(Counter(years).most_common())

        print(f'ТОП ФИЛЬМОВ С ТЕГАМИ ПО ГОДАМ:')
        place = 1
        for year, count in year_distribution.items():
            print(f'{year} : {count} тегов')
            place +=1  

        return year_distribution


# ==================== ТЕСТЫ ====================

class TestLinks:
    """Тесты для класса Links"""
    
    @pytest.fixture
    def links_obj(self):
        """Создание объекта Links с тестовыми данными"""
        links_path = '../datasets/ml-latest-small-1000/links_1000.csv'
        movies_path = '../datasets/ml-latest-small-1000/movies_1000.csv'
        
        if not os.path.exists(links_path) or not os.path.exists(movies_path):
            pytest.skip("Тестовые файлы не найдены")
        
        return Links(links_path, movies_path)
    
    @pytest.fixture
    def links_obj_with_imdb(self, links_obj):
        """Объект Links с загруженными данными IMDb"""
        movie_ids = list(links_obj.links.keys())[:3]
        fields = ["Director", "Budget", "Cumulative Worldwide Gross", "Runtime"]
        links_obj.imdb_info = links_obj.get_imdb(movie_ids, fields)
        return links_obj
    
    def test_init_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            Links('non_existent_file.csv')
    
    def test_init_empty_file(self, tmp_path):
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")
        with pytest.raises(FileNotFoundError):
            Links(str(empty_file))
    
    def test_load_links(self, links_obj):
        assert hasattr(links_obj, 'links')
        assert isinstance(links_obj.links, dict)
    
    def test_load_movies(self, links_obj):
        assert hasattr(links_obj, 'movies')
        assert isinstance(links_obj.movies, dict)
    
    def test_get_imdb_type_errors(self, links_obj):
        with pytest.raises(TypeError):
            links_obj.get_imdb("not_a_list", ["Director"])
        with pytest.raises(TypeError):
            links_obj.get_imdb([1, 2], "not_a_list")
    
    def test_get_imdb_structure(self, links_obj_with_imdb):
        """Проверка структуры результата get_imdb"""
        result = links_obj_with_imdb.imdb_info
        assert isinstance(result, list)
        if result:
            # Проверяем, что каждый элемент - это список
            assert isinstance(result[0], list)
            # Первый элемент должен быть movie_id (int)
            assert isinstance(result[0][0], int)
    
    def test_top_directors_invalid_n(self, links_obj_with_imdb):
        with pytest.raises(ValueError):
            links_obj_with_imdb.top_directors(0)
        with pytest.raises(ValueError):
            links_obj_with_imdb.top_directors(-5)
    
    def test_top_directors_return_type(self, links_obj_with_imdb):
        """Проверка типа возвращаемого значения top_directors"""
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        # Временно сохраняем старую структуру и преобразуем для теста
        old_imdb_info = links_obj_with_imdb.imdb_info
        # Преобразуем список списков в список словарей для теста
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.top_directors(3)
            assert isinstance(result, dict)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def _convert_to_dict_list(self, list_of_lists):
        """Преобразует список списков в список словарей"""
        if not list_of_lists:
            return []
        
        # Определяем заголовки на основе первого элемента
        # Формат: [movie_id, Title, Director, Budget, Cumulative Worldwide Gross, Runtime]
        headers = ['movie_id', 'Title', 'Director', 'Budget', 'Cumulative Worldwide Gross', 'Runtime']
        
        result = []
        for item in list_of_lists:
            if isinstance(item, list):
                movie_dict = {}
                for i, header in enumerate(headers):
                    if i < len(item):
                        movie_dict[header] = item[i]
                result.append(movie_dict)
        return result
    
    def test_top_directors_sorted(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.top_directors(5)
            values = list(result.values())
            if values:
                assert values == sorted(values, reverse=True)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_most_expensive_invalid_n(self, links_obj_with_imdb):
        with pytest.raises(ValueError):
            links_obj_with_imdb.most_expensive(0)
        with pytest.raises(ValueError):
            links_obj_with_imdb.most_expensive(-3)
    
    def test_most_expensive_return_type(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.most_expensive(3)
            assert isinstance(result, dict)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_most_expensive_sorted(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.most_expensive(5)
            values = list(result.values())
            if values:
                assert values == sorted(values, reverse=True)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_most_profitable_invalid_n(self, links_obj_with_imdb):
        with pytest.raises(ValueError):
            links_obj_with_imdb.most_profitable(0)
        with pytest.raises(ValueError):
            links_obj_with_imdb.most_profitable(-2)
    
    def test_most_profitable_return_type(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.most_profitable(3)
            assert isinstance(result, dict)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_most_profitable_sorted(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.most_profitable(5)
            values = list(result.values())
            if values:
                assert values == sorted(values, reverse=True)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_longest_invalid_n(self, links_obj_with_imdb):
        with pytest.raises(ValueError):
            links_obj_with_imdb.longest(0)
        with pytest.raises(ValueError):
            links_obj_with_imdb.longest(-1)
    
    def test_longest_return_type(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.longest(3)
            assert isinstance(result, dict)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_longest_sorted(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.longest(5)
            values = list(result.values())
            if values:
                assert values == sorted(values, reverse=True)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_top_cost_per_minute_invalid_n(self, links_obj_with_imdb):
        with pytest.raises(ValueError):
            links_obj_with_imdb.top_cost_per_minute(0)
        with pytest.raises(ValueError):
            links_obj_with_imdb.top_cost_per_minute(-4)
    
    def test_top_cost_per_minute_return_type(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.top_cost_per_minute(3)
            assert isinstance(result, dict)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_top_cost_per_minute_sorted(self, links_obj_with_imdb):
        if not links_obj_with_imdb.imdb_info:
            pytest.skip("Нет данных IMDb")
        old_imdb_info = links_obj_with_imdb.imdb_info
        links_obj_with_imdb.imdb_info = self._convert_to_dict_list(old_imdb_info)
        try:
            result = links_obj_with_imdb.top_cost_per_minute(5)
            values = list(result.values())
            if values:
                assert values == sorted(values, reverse=True)
        finally:
            links_obj_with_imdb.imdb_info = old_imdb_info
    
    def test_ensure_data_loaded_raises_error(self, links_obj):
        """Тест: _ensure_data_loaded вызывает ошибку при отсутствии данных"""
        with pytest.raises(ValueError, match="Нет данных в imdb_info"):
            links_obj.top_directors(5)


class TestRatings:
    """Тесты для класса Ratings"""
    
    @pytest.fixture
    def ratings_obj(self):
        ratings_path = '../datasets/ml-latest-small-1000/ratings_1000.csv'
        
        if not os.path.exists(ratings_path):
            pytest.skip("Тестовый файл ratings_1000.csv не найден")
        
        return Ratings(ratings_path)
    
    def test_init_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            Ratings('non_existent_file.csv')
    
    def test_init_empty_file(self, tmp_path):
        empty_file = tmp_path / "empty_ratings.csv"
        empty_file.write_text("")
        with pytest.raises(FileNotFoundError):
            Ratings(str(empty_file))
    
    def test_get_content_structure(self, ratings_obj):
        assert hasattr(ratings_obj, 'content')
        assert isinstance(ratings_obj.content, list)
        if ratings_obj.content:
            assert 'userId' in ratings_obj.content[0]
            assert 'movieId' in ratings_obj.content[0]
            assert 'rating' in ratings_obj.content[0]
            assert 'timestamp' in ratings_obj.content[0]
    
    def test_users_attribute(self, ratings_obj):
        assert hasattr(ratings_obj, 'users')
        assert hasattr(ratings_obj.users, 'dist_by_year')
        assert hasattr(ratings_obj.users, 'dist_by_rating')
        assert hasattr(ratings_obj.users, 'top_by_num_of_ratings')
    
    def test_dist_by_year_return_type(self, ratings_obj):
        result = ratings_obj.users.dist_by_year()
        assert isinstance(result, dict)
    
    def test_dist_by_rating_return_type(self, ratings_obj):
        result = ratings_obj.users.dist_by_rating()
        assert isinstance(result, dict)
    
    def test_top_by_num_of_ratings_return_type(self, ratings_obj):
        result = ratings_obj.users.top_by_num_of_ratings(3)
        assert isinstance(result, dict)
    
    def test_top_by_num_of_ratings_sorted(self, ratings_obj):
        result = ratings_obj.users.top_by_num_of_ratings(10)
        values = list(result.values())
        if values:
            assert values == sorted(values, reverse=True)
    
    def test_top_by_ratings_invalid_metric(self, ratings_obj):
        with pytest.raises(Exception):
            ratings_obj.users.top_by_ratings(5, metric='invalid')
    
    def test_top_by_ratings_average_return_type(self, ratings_obj):
        result = ratings_obj.users.top_by_ratings(3, metric='average')
        assert isinstance(result, dict)
    
    def test_top_by_ratings_median_return_type(self, ratings_obj):
        result = ratings_obj.users.top_by_ratings(3, metric='median')
        assert isinstance(result, dict)
    
    def test_top_controversial_return_type(self, ratings_obj):
        result = ratings_obj.users.top_controversial(3)
        assert isinstance(result, dict)
    
    def test_top_by_max_ratings_return_type(self, ratings_obj):
        result = ratings_obj.users.top_by_max_ratings(3)
        assert isinstance(result, dict)
    
    def test_dist_by_activity_return_type(self, ratings_obj):
        result = ratings_obj.users.dist_by_activity()
        assert isinstance(result, dict)
    
    def test_dist_by_activity_sorted(self, ratings_obj):
        result = ratings_obj.users.dist_by_activity()
        values = list(result.values())
        if values:
            assert values == sorted(values, reverse=True)
    
    def test_dist_by_metric_average_return_type(self, ratings_obj):
        result = ratings_obj.users.dist_by_metric(metric='average')
        assert isinstance(result, dict)
    
    def test_dist_by_metric_invalid(self, ratings_obj):
        with pytest.raises(Exception):
            ratings_obj.users.dist_by_metric(metric='invalid')


class TestTags:
    """Тесты для класса Tags"""
    
    @pytest.fixture
    def tags_obj(self):
        tags_path = '../datasets/ml-latest-small-1000/tags_1000.csv'
        
        if not os.path.exists(tags_path):
            pytest.skip("Тестовый файл tags_1000.csv не найден")
        
        return Tags(tags_path)
    
    def test_init_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            Tags('non_existent_file.csv')
    
    def test_init_empty_file(self, tmp_path):
        empty_file = tmp_path / "empty_tags.csv"
        empty_file.write_text("")
        with pytest.raises(FileNotFoundError):
            Tags(str(empty_file))
    
    def test_most_words_invalid_n(self, tags_obj):
        with pytest.raises(ValueError):
            tags_obj.most_words(0)
        with pytest.raises(ValueError):
            tags_obj.most_words(-5)
    
    def test_most_words_return_type(self, tags_obj):
        result = tags_obj.most_words(3)
        assert isinstance(result, dict)
    
    def test_most_words_sorted(self, tags_obj):
        result = tags_obj.most_words(10)
        values = list(result.values())
        if values:
            assert values == sorted(values, reverse=True)
    
    def test_longest_invalid_n(self, tags_obj):
        with pytest.raises(ValueError):
            tags_obj.longest(0)
        with pytest.raises(ValueError):
            tags_obj.longest(-3)
    
    def test_longest_return_type(self, tags_obj):
        result = tags_obj.longest(3)
        assert isinstance(result, list)
    
    def test_longest_sorted(self, tags_obj):
        result = tags_obj.longest(10)
        lengths = [len(tag) for tag in result]
        if lengths:
            assert lengths == sorted(lengths, reverse=True)
    
    def test_most_words_and_longest_invalid_n(self, tags_obj):
        with pytest.raises(ValueError):
            tags_obj.most_words_and_longest(0)
    
    def test_most_words_and_longest_return_type(self, tags_obj):
        result = tags_obj.most_words_and_longest(3)
        assert isinstance(result, list)
    
    def test_most_popular_invalid_n(self, tags_obj):
        with pytest.raises(ValueError):
            tags_obj.most_popular(0)
    
    def test_most_popular_return_type(self, tags_obj):
        result = tags_obj.most_popular(3)
        assert isinstance(result, dict)
    
    def test_most_popular_sorted(self, tags_obj):
        result = tags_obj.most_popular(10)
        values = list(result.values())
        if values:
            assert values == sorted(values, reverse=True)
    
    def test_tags_with_invalid_word(self, tags_obj):
        with pytest.raises(ValueError):
            tags_obj.tags_with("")
    
    def test_tags_with_return_type(self, tags_obj):
        result = tags_obj.tags_with("action")
        assert isinstance(result, list)
    
    def test_tags_with_sorted(self, tags_obj):
        result = tags_obj.tags_with("a")
        assert result == sorted(result)
    
    def test_dist_by_year_return_type(self, tags_obj):
        result = tags_obj.dist_by_year()
        assert isinstance(result, dict)


# Существующие тесты для Movies (оставляем как есть)
class TestMovies:
    @pytest.fixture
    def movies_obj(self):
        path = os.path.join('..', 'datasets', 'ml-latest-small-1000', 'movies_1000.csv')
        return Movies(path)
    
    def test_return_types(self, movies_obj):
        assert isinstance(movies_obj.dist_by_release(), dict)
        assert isinstance(movies_obj.dist_by_genres(), dict)
        assert isinstance(movies_obj.most_genres(5), dict)
    
    def test_sorting_order(self, movies_obj):
        dist = movies_obj.dist_by_genres()
        counts = list(dist.values())
        assert counts == sorted(counts, reverse=True)
    
    def test_exceptions(self):
        with pytest.raises(FileNotFoundError):
            Movies('invalid_path.csv')
    
    def test_bonus_franchises(self, movies_obj):
        res = movies_obj.get_top_franchises(n=1)
        assert isinstance(res, dict)