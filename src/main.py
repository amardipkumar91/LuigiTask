import luigi
import time
import os

class MakeDirectory(luigi.Task):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)


class HelloTask(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return [MakeDirectory(path=os.path.dirname(self.path))]

    def run(self):
       
        with open(self.path, 'w') as hello_file:
            hello_file.write("hello")
            hello_file.close()
    
    def output(self):
        return luigi.LocalTarget(self.path)

class WorldTask(luigi.Task):
    path = luigi.Parameter()
    def requires(self):
        return [MakeDirectory(path=os.path.dirname(self.path))]

    def run(self):
        with open(self.path, 'w') as hello_file:
            hello_file.write("world")
            hello_file.close()
    
    def output(self):
        return luigi.LocalTarget(self.path)

class HelloWorldTask(luigi.Task):

    id = luigi.Parameter(default='test')

    def requires(self):
        return [HelloTask(path = 'result/{}/hello.txt'.format(self.id)), \
            WorldTask(path = 'result/{}/world.txt'.format(self.id))]

    def run(self):
 
        with open(self.input()[0].path, 'r') as hello_file:
            hello = hello_file.read()
        with open(self.input()[1].path, 'r') as world_file:
            world = world_file.read()

        with open(self.output().path, 'w') as output_file:
            content = '{} {}'.format(hello, world)
            output_file.write(content)
            output_file.close()
    
    def output(self):
        path = 'result/{}/hello_world.txt'.format(self.id)
        return luigi.LocalTarget(path)


if __name__ == '__main__':
    luigi.run()