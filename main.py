# """
# Утилита для подсчёта статистики для стилометрических характеристик.
# """
# import json
# import traceback
#
# from textblob import TextBlob
# import pandas as pd
#
# from multiprocessing.dummy import Pool as ThreadPool
#
# ordered_features = [
#     "number_of_alphabets",
#     "number_of_characters",
#     "number_of_words",
#     "number_of_sentence",
#     "average_sentence_length_by_character",
#     "average_sentence_length_by_word",
#     "average_word_length"
# ]
#
#
# count = 0
# complete = 0
#
#
# def generate_statistics(files, output_path_general):
#     """
#    входная точка в утилиту, которые формирует статискику для всех файлов
#
#    Parameters:
#
#    files: список файлов, подаваемых на вход утилите для дальнейшей обработки
#    output_path_general: имя файла, где будет располагаться результат работы утилиты по общим характеристикам
#    output_path_letters_punc: имя файла, где будет распологаться результат работы утилиты по отдельным буквам и знакам
#    """
#
#     global count
#     count = len(files)
#
#     open(output_path_general, 'w').close()
#
#     # Итерируем список файлов и результат статистики пишем в качестве строки в файл
#
#     pool = ThreadPool(5)
#     results = pool.starmap(generate_statistic, zip(files))
#
#     pool.close()
#     pool.join()
#
#
#
#     file_result = []
#
#     for result in results:
#         try:
#             result_list = []
#             for f in ordered_features:
#                 result_list.append(str(result[f]))
#             file_result.append(result_list)
#         except Exception as e:
#             print(e)
#             print(traceback.format_exc())
#
#     # задаём формат csv-файла для статистики по общим характеристикам
#     table_general = pd.DataFrame(file_result, index=files,
#                                  columns=["number_of_alphabets",
#                                           "number_of_characters",
#                                           "number_of_words",
#                                           "number_of_sentence",
#                                           "average_sentence_length_by_character",
#                                           "average_sentence_length_by_word",
#                                           "average_word_length"
#                                           ])
#     table_general.to_csv(output_path_general, header=True, index=True)
#
#
# def generate_statistic(file):
#     """
#    генерация общей статистики для отдельного файла
#
#    Parameters:
#
#    file: файл для обработки
#    """
#     global count, complete
#
#     text = get_text_file(file)
#     splited_text = text.split("\n")
#
#     # подготовка текста
#     feature_dict = get_feature_dict()
#     blob = TextBlob(text)
#     sentences = blob.sentences
#     words = blob.words
#     number_of_words = len(words)
#
#     # определение количества символов (буквы, цифры, пробелы, знаки пунктуации, БЕЗ переноса строки!) и отдельно количества букв
#     number_of_characters = 0
#     number_of_alphabets = 0
#     for elem in splited_text:
#         for character in elem:
#             number_of_characters += 1
#             if character.isalpha():
#                 number_of_alphabets += 1
#
#     feature_dict["number_of_characters"] = number_of_characters
#     feature_dict["number_of_alphabets"] = number_of_alphabets
#
#     # определение кол-ва предложений в тексте
#     number_of_sentence = len(blob.sentences)
#     feature_dict["number_of_sentence"] = number_of_sentence
#
#     # определение кол-ва слов в тексте
#     feature_dict["number_of_words"] = number_of_words
#
#     # определение средней длины предложения (по символьно)
#     average_sentence_length_by_character = sum((map(lambda sentence: len(sentence), sentences))) / number_of_sentence
#     feature_dict["average_sentence_length_by_character"] = average_sentence_length_by_character
#
#     # определение средней длины предложения (по словам)
#     general_count = 0
#     for sentence in sentences:
#         sentence_blob = TextBlob(str(sentence))
#         general_count += len(sentence_blob.words)
#     average_sentence_length_by_word = general_count / len(sentences)
#     feature_dict["average_sentence_length_by_word"] = average_sentence_length_by_word
#
#     # определение средней длины слов
#     average_word_length = sum((map(lambda word: len(word), words))) / number_of_words
#     feature_dict["average_word_length"] = average_word_length
#
#     complete += 1
#     print(complete, ' / ', count)
#
#     return feature_dict
#
#
#
#
# def get_text_file(file):
#     f = open(file, encoding="utf-8")
#     text = " ".join(f.read().split("\n"))
#     return text
#
#
# def get_feature_dict():
#     """
#    Возвращаем стандартный словарь с фичами, где ключ - название фичи, значение - их количество
#
#    """
#     return {
#         "number_of_characters": 0,
#         "number_of_alphabets": 0,
#         "number_of_alphabets_az": 0,
#         "number_of_words": 0,
#         "number_og_sentence": 0,
#         "average_word_length": 0,
#         "average_sentence_length_by_character": 0,
#         "average_sentence_length_by_word": 0
#     }
#
#
# # данные
# files1 = [
#     "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment.txt",
#     "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment_2.txt",
#     "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment_3.txt",
#     "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment_4.txt"
# ]
#
# output_path_general = "D:/Projects/FunctionalProgramming/output_path_general.csv"
#
# generate_statistics(files1, output_path_general)
import traceback
from multiprocessing.pool import ThreadPool
from textblob import TextBlob
import pandas as pd

# (данные) список столбцов для csv-таблицы
ordered_features = [
    "number_of_alphabets",
    "number_of_characters",
    "number_of_words",
    "number_of_sentence",
    "average_sentence_length_by_character",
    "average_sentence_length_by_word",
    "average_word_length"
]

count = 0
complete = 0


# (действие) входная точка в утилиту. Итерируем список файлов и результат статистики пишем в качестве строки в файл
def generate_statistics(files, output_path_general):
    global count
    count = len(files)
    pool = ThreadPool(5)
    results = pool.starmap(generate_statistic, zip(files))
    pool.close()
    pool.join()
    file_result = make_file_result(results)
    write_result(file_result, files, output_path_general)


# (вычисление) преобразуем список словарей в список списков
def make_file_result(results):
    file_result = []

    for result in results:
        try:
            result_list = []
            for f in ordered_features:
                result_list.append(str(result[f]))
            file_result.append(result_list)
        except Exception as e:
            print(e)
            print(traceback.format_exc())

    return file_result


# (действие) записываем результат в csv-таблицу
def write_result(file_result, files, output_path_general):
    table_general = pd.DataFrame(file_result, index=files,
                                 columns=["number_of_alphabets",
                                          "number_of_characters",
                                          "number_of_words",
                                          "number_of_sentence",
                                          "average_sentence_length_by_character",
                                          "average_sentence_length_by_word",
                                          "average_word_length"
                                          ])
    table_general.to_csv(output_path_general, header=True, index=True)


# (вычисление) генерация общей статистики для отдельного файла
def generate_statistic(file):
    feature_dict = get_feature_dict()

    splited_text, sentences, words = text_preparing(file)

    # определение кол-ва слов в тексте
    feature_dict["number_of_words"] = len(words)

    # определение количества символов (буквы, цифры, пробелы, знаки пунктуации, БЕЗ переноса строки!) и отдельно количества букв
    feature_dict["number_of_characters"], feature_dict["number_of_alphabets"] = character_alphabet_count(splited_text)

    # определение кол-ва предложений в тексте
    feature_dict["number_of_sentence"] = len(sentences)

    # определение средней длины предложения (посимвольно)
    feature_dict["average_sentence_length_by_character"] = averenge_lenght_of_sentence_by_character(sentences)

    # определение средней длины предложения (по словам)
    feature_dict["average_sentence_length_by_word"] = averenge_lenght_of_sentence_by_words(sentences)

    # определение средней длины слов
    feature_dict["average_word_length"] = averenge_word_lenght(words)

    text_handled_count()

    return feature_dict


# (действие) вывод кол-ва обработанных текстов
def text_handled_count():
    global count, complete
    complete += 1
    print(complete, ' / ', count)


# (вычисление) подготовка текста
def text_preparing(file):
    text = get_text_file(file)
    splited_text = text.split("\n")
    blob = TextBlob(text)
    sentences = blob.sentences
    words = blob.words
    return splited_text, sentences, words


# (действие) получение текста из файла
def get_text_file(file):
    f = open(file, encoding="utf-8")
    text = " ".join(f.read().split("\n"))
    return text


# (вычисление) определение количества символов (буквы, цифры, пробелы, знаки пунктуации, БЕЗ переноса строки!) и отдельно количества букв
def character_alphabet_count(splited_text):
    number_of_characters = 0
    number_of_alphabets = 0
    for elem in splited_text:
        for character in elem:
            number_of_characters += 1
            if character.isalpha():
                number_of_alphabets += 1
    return number_of_characters, number_of_alphabets


# (вычисление) определение средней длины предложения (посимвольно)
def averenge_lenght_of_sentence_by_character(sentences):
    return sum((map(lambda sentence: len(sentence), sentences))) / len(sentences)


# (вычисление) определение средней длины предложения (по словам)
def averenge_lenght_of_sentence_by_words(sentences):
    general_count = 0
    for sentence in sentences:
        sentence_blob = TextBlob(str(sentence))
        general_count += len(sentence_blob.words)
    general_count /= len(sentences)
    return general_count


# (вычисление) определение средней длины слов
def averenge_word_lenght(words):
    return sum((map(lambda word: len(word), words))) / len(words)


# (данные) возвращаем стандартный словарь с фичами, где ключ - название фичи, значение - их количество
def get_feature_dict():
    return {
        "number_of_characters": 0,
        "number_of_alphabets": 0,
        "number_of_words": 0,
        "number_of_sentence": 0,
        "average_word_length": 0,
        "average_sentence_length_by_character": 0,
        "average_sentence_length_by_word": 0
    }


# данные (список обрабатываемых файлов)
files1 = [
    "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment.txt",
    "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment_2.txt",
    "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment_3.txt",
    "D:/Projects/FunctionalProgramming/Corpora/1846-Достоевский-Двойник-fragment_4.txt"
]

# данные (файл, куда будем записывать результат работы программы)
output_path_general = "D:/Projects/FunctionalProgramming/output_path_general.csv"

generate_statistics(files1, output_path_general)
