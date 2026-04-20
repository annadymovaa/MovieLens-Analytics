#Разрешённые импорты: os, sys, urllib, requests, beautifulsoup, json, pytest, collections, functools, datetime, re.
from datetime import datetime
from collections import Counter


class Ratings:
    """
    Analyzing data from ratings.csv
    """
    def __init__(self, path_to_the_file='../datasets/ml-latest-small-1000/ratings_1000.csv'):
        self.filepath = path_to_the_file 
        self.movies = self.Users(self)
        self.get_content()

    def get_content(self):
        with open(self.filepath, 'r', encoding='utf-8') as file:
            self.content = file.readlines()

    class Movies:  
        def __init__(self, outer_instance):
            # Сохраняем ссылку на внешний объект
            self.outer = outer_instance  

        def dist_by_year(self):
            #{year : count} по ключам по возрастанию

            years = []
            for i in range(1, 1001):
                line = self.outer.content[i].split(',')
                timestamp = int(line[3])
                m_year = datetime.fromtimestamp(timestamp).year
                years.append(m_year)
            count_by_year = Counter(years)
            ratings_by_year = dict(sorted(count_by_year.items()))

            return ratings_by_year
        
        def dist_by_rating(self):
            #{rating : count} по рейтингу по возрастанию

            ratings = []
            for i in range(1, 1001):
                line = self.outer.content[i].split(',')
                rating = float(line[2])
                ratings.append(rating)
            count_by_rating = Counter(ratings)
            ratings_distribution = dict(sorted(count_by_rating.items()))

            return ratings_distribution
        
        def top_by_num_of_ratings(self, n):
            #{movie_title : num of ratings} по номерам по убыванию

            mov_id_rat = dict()
            for i in range(1, 1001):
                line = self.outer.content[i].split(',')
                movie_id = line[1]
                if movie_id not in mov_id_rat:
                    mov_id_rat[movie_id] = 1
                else:
                    mov_id_rat[movie_id] += 1
            movie_ids = dict(sorted(mov_id_rat.items(), key=lambda item: item[1], reverse=True))

            top_movie_ids = dict(list(movie_ids.items())[:n])

            # кажется, что для этого сойдет какой-нибудь метод из класса Movies
            # пока что оставлю так
            # with open('../datasets/ml-latest-small-1000/movies_1000.csv', 'r', encoding='utf-8') as file:
            #     movies = file.readlines()
            # for movie_id in top_movie_ids:
            #     for movie in movies:
            #         line = movie.split(',')
            #         if movie_id == line[0]:

            top_movies = top_movie_ids
            return top_movies
        
        def dict_of_ids_rats(self):
            self.mov_id_rat = dict()
            for i in range(1, 1001):
                line = self.outer.content[i].split(',')
                movie_id = line[1] 
                rating = float(line[2])   
                if movie_id not in self.mov_id_rat:
                    self.mov_id_rat[movie_id] = list()
                self.mov_id_rat[movie_id].append(rating)
        
        def top_by_ratings(self, n, metric='average'):
            self.dict_of_ids_rats()
            movie_id_metrics = dict()
            for movie in self.mov_id_rat:
                list_of_rats = self.mov_id_rat[movie]
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
            
                movie_id_metrics[movie] = round(metric_v, 2)
            
            movie_ids = dict(sorted(movie_id_metrics.items(), key=lambda item: item[1], reverse=True))
            top_movie_ids = dict(list(movie_ids.items())[:n])
            
            top_movies = top_movie_ids
            #TODO: добавить замену айди на название фильма
              
            """
            The method returns top-n movies by the average or median of the ratings.
            It is a dict where the keys are movie titles and the values are metric values.
            Sort it by metric descendingly.
            The values should be rounded to 2 decimals.
            """
            return top_movies
        def top_controversial(self, n):
            self.dict_of_ids_rats()
            movie_id_metrics = dict()
            for movie in self.mov_id_rat:
                list_of_rats = self.mov_id_rat[movie]
                variance = 0
                rat_count = len(list_of_rats)
                if rat_count != 1:
                    cumulator = 0
                    mean = sum(list_of_rats) / rat_count
                    for rating in list_of_rats:
                        cumulator += (rating - mean) ** 2
                    variance = cumulator / rat_count
                movie_id_metrics[movie] = round(variance, 2)

            movie_ids = dict(sorted(movie_id_metrics.items(), key=lambda item: item[1], reverse=True))
            top_movie_ids = dict(list(movie_ids.items())[:n])
            
            top_movies = top_movie_ids
            #TODO: добавить замену айди на название фильма

            """
            The method returns top-n movies by the variance of the ratings.
            It is a dict where the keys are movie titles and the values are the variances.
          Sort it by variance descendingly.
            The values should be rounded to 2 decimals.
            """
            return top_movies

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
            for i in range(1, 1001):
                line = self.outer.content[i].split(',')
                user = line[0]
                users.append(user)
            count_by_ratings = Counter(users)
            ratings_by_user = dict(sorted(count_by_ratings.items(), key=lambda item: item[1], reverse=True))

            return ratings_by_user
        
        def dict_of_user_rats(self):
            self.user_id_rat = dict()
            for i in range(1, 1001):
                line = self.outer.content[i].split(',')
                user_id = line[0] 
                rating = float(line[2])   
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
            
            return user_ids

        def top_controversial_users(self, n):
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

            return top_user_ids

