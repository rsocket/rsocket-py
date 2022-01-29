from setuptools import setup, find_packages

setup(
    name='rsocket',
    version='0.3',
    description='Python RSocket library',
    url='https://github.com/rsocket/rsocket-py',
    author='Gabriel Shaar',
    author_email='gabis@precog.co',
    license='MIT',
    packages=find_packages(exclude=['examples', 'tests', 'docs']),
    zip_safe=True,
    extra_required={
        'rx': {'Rx >= 3.0.0'},
        'websocket': {'aiohttp == 3.8.1'},
        'quart': {'quart == 0.16.2'}
    },
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])
