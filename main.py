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
    results = handle_files(files)
    file_result = make_file_result(results)
    write_result(file_result, files, output_path_general)


# (действие) итерируем список файлов и для каждого из них вызываем функцию generate_statistic
def handle_files(files):
    global count
    count = len(files)
    pool = ThreadPool(5)
    results = pool.starmap(generate_statistic, zip(files))
    pool.close()
    pool.join()
    return results


# (вычисление) преобразуем список словарей в список списков
def make_file_result(results):
    file_result = []

    for result in results:
        result_list = []
        for f in ordered_features:
            result_list.append(str(result[f]))
        file_result.append(result_list)
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


# (действие) генерация общей статистики для отдельного файла
def generate_statistic(file):
    feature_dict = dict()

    text = get_text_file(file)

    sentences, words = get_sentences_words(text)

    # определение кол-ва слов и предложений в тексте
    feature_dict["number_of_words"], feature_dict["number_of_sentence"] = get_number_sentences_words(words, sentences)

    # определение количества символов (буквы, цифры, пробелы, знаки пунктуации, БЕЗ переноса строки!) и отдельно количества букв
    feature_dict["number_of_characters"], feature_dict["number_of_alphabets"] = character_alphabet_count(text)

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


# (вычисление) получение предложений и слов
def get_sentences_words(text):
    blob = TextBlob(text)
    sentences = blob.sentences
    words = blob.words

    return sentences, words


# (вычисление) получение количества предложений и слов
def get_number_sentences_words(words, sentences):
    return len(words), len(sentences)


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
            if character != "\n":
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
