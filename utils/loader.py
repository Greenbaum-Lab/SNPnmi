import datetime
from itertools import cycle
from shutil import get_terminal_size
from threading import Thread
from time import sleep


class Loader:
    def __init__(self, is_done_method=None, desc="Loading...",  end="Done!", timeout=0.15):
        """
        A loader-like context manager

        Args:
            desc (str, optional): The loader's description. Defaults to "Loading...".
            end (str, optional): Final print. Defaults to "Done!".
            timeout (float, optional): Sleep time between prints. Defaults to 0.1.
        """
        self.desc = desc
        self.end = end
        self.timeout = timeout
        self.is_done_method = is_done_method
        self._thread = Thread(target=self._animate, daemon=True)
        self.steps = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]
        self.done = False

    def start(self):
        self._thread.start()
        return self

    def _animate(self):
        informative_text = ""
        for c in cycle(self.steps):
            if c == "⣾":
                if self.is_done_method:
                    informative_text = self.is_done_method()
            if self.done:
                break
            print(f"\r{self.desc} {informative_text}{c}  ", flush=True, end="")
            sleep(self.timeout)

    def __enter__(self):
        self.start()

    def stop(self):
        self.done = True
        cols = get_terminal_size((80, 20)).columns
        print("\r" + " " * cols, end="", flush=True)
        print(f"\r{self.end}", flush=True)

    def __exit__(self, exc_type, exc_value, tb):
        # handle exceptions with those variables ^
        self.stop()

class Timer:
    def __init__(self, text=""):
        self.text = text

    def __enter__(self):
        self.begin = datetime.datetime.now()

    def __exit__(self, type, value, traceback):
        print(f"{self.text} took {self.format_delta(self.begin, datetime.datetime.now())}")

    def format_delta(self, start, end):

        # Time in microseconds
        one_day = 86400000000
        one_hour = 3600000000
        one_second = 1000000
        one_millisecond = 1000

        delta = end - start

        build_time_us = delta.microseconds + delta.seconds * one_second + delta.days * one_day

        days = 0
        while build_time_us > one_day:
            build_time_us -= one_day
            days += 1

        if days > 0:
            time_str = "%.2fd" % (days + build_time_us / float(one_day))
        else:
            hours = 0
            while build_time_us > one_hour:
                build_time_us -= one_hour
                hours += 1
            if hours > 0:
                time_str = "%.2fh" % (hours + build_time_us / float(one_hour))
            else:
                seconds = 0
                while build_time_us > one_second:
                    build_time_us -= one_second
                    seconds += 1
                if seconds > 0:
                    time_str = "%.2fs" % (seconds + build_time_us / float(one_second))
                else:
                    ms = 0
                    while build_time_us > one_millisecond:
                        build_time_us -= one_millisecond
                        ms += 1
                    time_str = "%.2fms" % (ms + build_time_us / float(one_millisecond))
        return time_str