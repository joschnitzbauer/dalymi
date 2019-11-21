from setuptools import setup


with open("README.md", "r") as f:
    long_description = f.read()


setup(author='Joerg Schnitzbauer',
      author_email='joschnitzbauer@gmail.com',
      description=('[data like you mean it] A lightweight, data-focused and non-opinionated pipeline manager written '
                   'in and for Python.'),
      long_description=long_description,
      long_description_content_type='text/markdown',
      license='MIT',
      name='dalymi',
      packages=['dalymi'],
      url='https://github.com/joschnitzbauer/dalymi',
      version='0.1.5')
