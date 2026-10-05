# file chunks
import os

# local file chunks
class Pieces:
    # set up file storage
    def __init__(self, root, info, config):
        self.config = config
        # set peer folder
        directory = root / f'peer_{info.id}'
        directory.mkdir(exist_ok=True)
        # set file paths
        self.final = directory / config.filename
        self.partial = directory / (config.filename + '.part')
        self.have = set(range(config.count)) if info.seed else set()
        # use seed file
        if info.seed:
            # check seed file size
            if not self.final.is_file() or self.final.stat().st_size != config.size:
                raise ValueError(f'Seed file missing or wrong size: {self.final}')
            self.file = self.final.open('rb')
        # create empty partial file
        else:
            self.file = self.partial.open('w+b')
            self.file.truncate(config.size)

    # check file done
    @property
    def complete(self):
        return len(self.have) == self.config.count

    # read chunk
    def read(self, index):
        # move to chunk offset
        self.file.seek(index * self.config.piece_size)
        return self.file.read(self.config.length(index))

    # save chunk
    def put(self, index, data):
        # check chunk size
        if len(data) != self.config.length(index):
            raise ValueError('Wrong piece length')
        # skip duplicate chunk
        if index in self.have:
            return False
        # move to chunk offset
        self.file.seek(index * self.config.piece_size)
        self.file.write(data)
        self.have.add(index)
        # finish local file
        if self.complete:
            # flush complete file
            self.file.flush()
            os.fsync(self.file.fileno())
            self.file.close()
            os.replace(self.partial, self.final)
            self.file = self.final.open('rb')
        return True

    # close file
    def close(self):
        self.file.close()
