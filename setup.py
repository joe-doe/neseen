from distutils.core import setup

setup(name='neseen',
      version='0.1',
      packages=['', ],
      license='',
      author='me',
      description='NEw SEarch ENgine',
      install_requires=[
            'bs4',
      ],
      package_data={
          'neseen': ['web/*']
      })
