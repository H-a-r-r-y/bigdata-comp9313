import math
from mrjob.job import MRJob, MRStep
from mrjob.compat import jobconf_from_env


class weightInvertedIndex(MRJob):
    #   params:
    #   line: a line in the file. format: [docID term term term term]
    def mapper(self, _, line):
        term_frequency = {}
        if line.strip() != "":
            terms = line.strip().split()
            docID = terms[0]
            for i in range(1, len(terms)):
                if terms[i] not in term_frequency:
                    term_frequency[terms[i]] = 1

                    # use special key idea as a counter
                    # use -1 as key, make sure it appears before docID = 0
                    yield terms[i].lower() + "," + "-1", 1
                else:
                    term_frequency[terms[i]] += 1

            # emit ["term,docID", tf]
            # concatenate the term and docID by ",", so that we can use "," as seperator for partition and sort
            for term in term_frequency:
                yield term.lower() + "," + docID, term_frequency[term]


    # define marginal to store the value of special key. (i.e the total number of occurrence of a term in all document)
    # variable in reducer_init will be shared by all reducers.
    # dont need to return
    def reducer_init(self):
        self.marginal = 0

    #   params:
    #   key: term,docID
    #   values: [tf, tf, tf, tf]
    def reducer(self, key, values):
        term,docID = key.split(",")

        # deal with special key, aggregate the marginal
        # 在我们利用合理的comparator进行sort 之后，我们是可以保证一下的点
            # 1. 相同的term 全部被map 到同一个reducer
            # 2. special key term + "-1" 的那个key 保证出现在 其他 term + docID 之前。sort by docID，-1 会排在其他docID前。
            # 3. 而且根据term sort 之后，相同的term是排在一起的。
                #e.g
                    # term1，-1， [1,1,1,1...]
                    # term1，docID1， tf
                    # term1，docID2， tf
                    # term1，docID3， tf
                    #
                    # term2，-1， [1,1,1,1..]
                    # term2，docID1， tf
                    # term2，docID2， tf
                    # term2，docID3， tf

        # 这样我们就可以先用special key 的 values，计算出这个term 在多少个文章中出现，并使用这个结果计算tfidf
        if docID == "-1":
            self.marginal = sum(values)
        else:
            # aggregate the term frequency, then use marginal to calculate the tfidf.
            tf = sum(values)
            tfidf = tf * math.log10(int(jobconf_from_env('myjob.settings.docnumber')) / self.marginal)
            yield term, docID + ", " + str(tfidf)


    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]

    SORT_VALUES = True

    # comparotor 用来对mapper输出的数据行进行排序，下面是先sort by key的第一个元素，然后sort by 第二个元素
    # partitionner 用来对mapper输出的数据行进行分区，下面是根据key的第一个元素哈希分区

    JOBCONF = {
        'mapreduce.map.output.key.field.separator': ',',
        'mapreduce.partition.keypartitioner.options': '-k1,1', # partition by term
        'partitioner': 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
        'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2n' # first sort by term,  secondary sort by docID numerically
    }


if __name__ == '__main__':
    weightInvertedIndex.run()





