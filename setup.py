from setuptools import setup, find_packages

setup(name='regla',
      version='0.0.1',
      description='Xyla Python rules engine',
      url='https://github.com/xyla-io/regla',
      author='Xyla',
      author_email='gklei89@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          "pandas",
          "numpy",
      ],
      zip_safe=False)
