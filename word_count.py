import re
#links = sc.textFile("s3://commoncrawl/crawl-data/CC-MAIN-2020-16/wet.paths.gz")
data = sc.textFile("s3://commoncrawl/crawl-data/CC-MAIN-2020-16/segments/1585370490497.6/wet/CC-MAIN-20200328074047-20200328104047-00000.warc.wet.gz")
flatData = data.flatMap(lambda line: re.split(' |, |. ',line))
onlyVirus = flatData.filter(lambda line: line =='koronawirus' or line =='pandemia' or line =='covid19' or line =='covid' or line =='wirus')
countv = onlyVirus.countByValue()
for word, count in countv.items():
    print("{} : {}".format(word, count))
