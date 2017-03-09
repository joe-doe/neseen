from distutils.core import setup

setup(name='neseen',
      version='0.1',
      packages=['', ],
      license='',
      author='me',
      description='NEw SEarch ENgine',
      install_requires=[
            'flask',
            'bs4',
            'pymongo',
            'elasticsearch',
            'aiohttp'
      ],
      package_data={
          'neseen': ['web/*']
      })
