from nltk.tokenize.regexp import RegexpTokenizer
# from nltk.corpus import stopwords
from nltk.stem.snowball import RussianStemmer
import codecs
from collections import Counter
import heapq


class AdParser:

    def __init__(self):
        self.unique = {"ru": "БбвГгДдЁёЖжЗзИиЙйЛлмнПптУФфЦцЧчШшЩщЪъЫыЬьЭэЮюЯя", "en": "DdFfGghIiJjLlNQqRrSstUVvWwYZz"}
        self.common = {"ru": "АаВЕеКкМНОоРрСсТуХхЗО1тиа@пь", "en": "AaBEeKkMHOoPpCcTyXx30imu@anb"}
        with codecs.open("sw.txt", "r", encoding="utf-8") as f_sw:  # использовал альтернативный список стоп-слов
            self.sw = [word.strip() for word in f_sw]

    def check_word(self, word):
        """
        Проверка слова на мошенничество
        :param word: слово на вход
        :return: новое слово или None, если все ОК
        """
        langs = ["ru", "en"]
        new_word = None
        for i, lang in enumerate(langs):
            opposite = langs[0] if i > 0 else langs[1]
            if self.has_special_chars(word, lang):
                for i, char in enumerate(word):
                    idx = self.common[opposite].find(char)
                    if idx > 0:
                        new_word = word[:i] + self.common[lang][idx] + word[i + 1:]
                break  # заканчиваем цикл, если поняли что это русское слово, иначе проверяем английский
        return new_word

    def has_special_chars(self, word, lang):
        """
        Проверяем на наличие уникальных для языка букв, таким образом определяем язык
        :param word: проверяемое слово
        :param lang: язык
        :return:
        """
        if not (lang == 'ru' or lang == 'en'):
            raise ValueError("Error while checking letters; language incorrect: " + lang)
        for c in word:
            if c in self.unique[lang]:
                return True
        return False

    def parse(self, fname):
        """
        Парсинг текста файла
        :param fname: имя файла
        :return: (<имя_файла>, тошнота, мошенничество)
        """
        density, fraud = 0, 0
        with codecs.open(fname, "r", encoding="utf-8") as f:
            text = f.read()
        tknz = RegexpTokenizer(pattern="[А-Яа-яA-zё]+")
        txt_list = tknz.tokenize(text)
        if txt_list:
            for i, word in enumerate(txt_list):
                new_word = self.check_word(word)
                if new_word:
                    txt_list[i] = new_word
                    fraud += 1

            txt_list = [word.lower() for word in txt_list if not (word.lower() in self.sw)]
            stemmer_ru = RussianStemmer()
            txt_list = [stemmer_ru.stem(token.lower()) for token in txt_list if len(token) > 1]
            dict_w = Counter(txt_list)
            top5 = heapq.nlargest(5, dict_w, key=dict_w.get)
            top5_count = sum([dict_w[word] for word in top5])
            density = top5_count/len(txt_list)
        # такой критерий (fraud > 2) был выбран на основании тестирования на имеющейся выборке
        # часто попадается такое, что в объявлении есть слова типа "ШxДхВ" которые мы не можем однозначно распознать
        # готов обсуждать этот критерий, возможно исправить каким то образом
        return fname, density, fraud > 2
