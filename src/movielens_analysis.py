#Разрешённые импорты: os, sys, urllib, requests, beautifulsoup, json, pytest, collections, functools, datetime, re.
from datetime import datetime
from collections import Counter


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
