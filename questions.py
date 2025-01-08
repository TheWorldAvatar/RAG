import json
from datetime import datetime

from common import *

# Question/answer dictionary fields
QADF_AGENT     = "agent"
QADF_ANSWERS   = "answers"
QADF_QUESTIONS = "questions"
QADF_TEXT      = "text"
QADF_TIMESTAMP = "timestamp"

class Answer:

    def __init__(self, text: str, agent: str, timestamp: datetime) -> None:
        self._text = text
        self._agent = agent
        self._timestamp = timestamp

    def to_dict(self) -> dict:
        return {
            QADF_TEXT: self._text,
            QADF_AGENT: self._agent,
            QADF_TIMESTAMP: self._timestamp.strftime(FMT_DATE_TIME)
        }

class Question:

    def __init__(self, text: str) -> None:
        self._text = text
        self._answers: list[Answer] = []

    def get_text(self) -> str:
        return self._text

    def add_answer(self, answer: Answer) -> None:
        self._answers.append(answer)

    def to_dict(self) -> dict:
        answers = [a.to_dict() for a in self._answers]
        return {
            QADF_TEXT: self._text,
            QADF_ANSWERS: answers
        }

class Questions:

    def __init__(self) -> None:
        self._content: list[Question] = []

    def find_question(self, text: str) -> Question:
        """
        If the text matches, returns a reference to an existing question
        object. Otherwise, returns none.
        """
        lookup = {q.get_text(): q for q in self._content}
        return lookup[text] if text in lookup else None

    def add_question(self, question: Question) -> None:
        """
        Appends a given question object to the list, without checking
        for duplicates.
        """
        self._content.append(question)

    def find_question_or_add_new(self, text: str) -> Question:
        """
        If the text matches, returns a reference to an existing question
        object. Otherwise, creates a new question object with the given
        text, appends it to the list, and returns a reference to it.
        """
        existing_q = self.find_question(text)
        if existing_q is not None:
            return existing_q
        else:
            new_q = Question(text)
            self.add_question(new_q)
            return new_q

    def to_dict(self) -> dict:
        qs = [q.to_dict() for q in self._content]
        return {QADF_QUESTIONS: qs}

    def load(self, filename: str) -> None:
        with open(filename, "r", encoding=ES_UTF_8) as infile:
            json_str = infile.read()
            for q_dict in json.loads(json_str)[QADF_QUESTIONS]:
                q = Question(q_dict[QADF_TEXT])
                for a_dict in q_dict[QADF_ANSWERS]:
                    q.add_answer(Answer(a_dict[QADF_TEXT], a_dict[QADF_AGENT],
                        datetime.strptime(a_dict[QADF_TIMESTAMP], FMT_DATE_TIME)))
                self.add_question(q)

    def save(self, filename: str) -> None:
        export_dict_to_json(self.to_dict(), filename)

#myqs = Questions()
#myqs.load("myqs.json")
#myqs.find_question_or_add_new("Why that?").add_answer(Answer("Because of Z.", "RAG", datetime.now()))
#myqs.save("myqs-resave.json")
