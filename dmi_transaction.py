import datetime


class DMITransaction:
    def __init__(self, extension, timestamp):
        self.extension: str = extension
        self.timestamp: datetime.datetime = timestamp
        self.user: str = None
        self.elapsed_time: int = -1
        self.error: str = ''
        self.incoming_length: int = -1
        self.outgoing_length: int = -1
        self.complete: bool = False
        self.calc_elapsed: int = -1

    def in_error(self):
        return self.elapsed_time > 10000 \
               or self.error \
               or self.incoming_length > 100000 \
               or self.outgoing_length > 100000\
               or self.calc_elapsed > 10

    def contains_any(self, pattern_list: list):
        return any([self.contains(x) for x in pattern_list])

    def contains(self, pattern):
        if self.error:
            return pattern in self.error
        else:
            return False

    def get_status_time(self):
        et = self.elapsed_time
        if self.elapsed_time == -1:
            et = 'unknown'
        if self.elapsed_time == -2:
            et = 'timeout'
        if self.elapsed_time == -3:
            et = 'pending'
        return et

    def __repr__(self):
        et = self.get_status_time()
        ic = self.incoming_length
        if self.incoming_length == -1:
            ic = 'unknown'
        og = self.outgoing_length
        if self.outgoing_length == -1:
            og = 'unknown'
        ce = int(self.calc_elapsed * 1000)
        if self.calc_elapsed == -1:
            ce = 'unknown'
        return f"{self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')} {self.extension} {et} {ce} {ic} {og} user: {self.user} error: {self.error}"