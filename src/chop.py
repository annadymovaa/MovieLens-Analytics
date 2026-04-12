if __name__ == '__main__':
    filenames = ['links', 'movies', 'ratings', 'tags']
    path = '../datasets/ml-latest-small'

    for filename in filenames:
        filepath = path + '/' + filename + '.csv'
        newfile = path + '-1000/' + filename + '_1000.csv'
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.readlines()
        with open(newfile, 'w', encoding='utf-8') as newfile:
            newfile.writelines(content[:1001])
