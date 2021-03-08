from setuptools import setup

setup(
    name='rsocket',
    version='1.0.0',
    description='Python RSocket library',
    url='https://github.com/RSocket/rsocket-py',
    author='Vijayan Rajan',
    author_email='',
    license='MIT',
    packages=[
        'rsocket',
        'reactivestreams',
        'reactivestreams.rx',
    ],
    zip_safe=True,
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
