# to run: navigate to the folder that includes the code and the movies.csv and run by: python3 test.py

# github: https://github.com/maryamrahdaran/code-challenge

# count the number of genres in each line
# argument: the genre field
# return: int - number of genre
# note: some moviesa doesn't have any genre
from prefect import task, Flow


def genreCount(genres):
    glist = genres.split("|")
    if glist[0] == '(no genres listed)':
        return 0
    else:
        return len(glist)


# make a list of genre dictionary for each line: {genres: count}, if the key(genre) already exists add 1 to its count, otherwise add the key with the count 1 to the dictionary
# argument: a line of file(line), a dictionary(dict_genre) to be made
# return: nothing

def make_genreDict(line, dict_genre):
    glist = line['genres'].split("|")
    for g in glist:
        if g in dict_genre:
            dict_genre[g] = dict_genre[g] + 1
        else:
            dict_genre[g] = 1


# find the most genre in a dictionary
# argument: a dictionary(dict_genre)
# return: a tuple (most genre, count of the most genre)

def find_max(dict_genre):
    max_genre_count = 0
    max_genre = ""
    for key, value in dict_genre.items():
        if max_genre_count < value:
            max_genre_count = value
            max_genre = key
    return (max_genre, max_genre_count)

@task
def extract(f=[]):
    with open('../pythonProject1/movies.csv', mode='r') as readFile:
        for row in csv.DictReader(readFile):
            yield row


@task
def transfer(reader, f=[]):
    num_lines = 0
    sum_genreCount = 0
    dict_genre = {}

    for line in reader:
        # make the dictionary of genres
        make_genreDict(line, dict_genre)

        # add the new count field to dictionary
        count = genreCount(line['genres'])
        line['genre_count'] = count

        # for calculating average
        sum_genreCount += count
        num_lines += 1
        yield line

    # find the most genre
    max_genre = find_max(dict_genre)
    print('Average is: {}'.format(sum_genreCount / num_lines))
    print('most common genre is {} with {} count.'.format(max_genre[0], max_genre[1]))

@task
def load(reader):
    with open('movie_enhanced.csv', mode='w') as writeFile:
        #?????????? manually writing headers
        writer = csv.DictWriter(writeFile, ["movieId", "title", "genres", "genre_count"])
        writer.writeheader()

        for line in reader:
            writer.writerow(line)


if __name__ == '__main__':
    import csv

    with Flow("ETL") as flow:
        reader = extract()
        transfer = transfer(reader)
        load(transfer)

        flow.run()
#not working????????
   # flow.visualize()


