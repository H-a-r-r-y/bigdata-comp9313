### We use value-to-key conversion to do this task, and let Hadoop do the sorting for us.
### Unfortunately, there is no GroupingComparator in MRJob. We have to process each pair in one reduce function.

from mrjob.job import MRJob

class ReverseEdgeDirection(MRJob):

    def mapper(self, _, value):
        fromNode, neighbor = value.split("\t", 1)
        nodes = neighbor.strip().split(" ")

        for node in nodes:
            yield node + "," + fromNode, ""    
    
    def reducer_init(self):
        self.neighbor=[]
        self.curNode=""
    
    # Note that the reduce design is based on the fact that the pairs come in order.
    def reducer(self, key, values):
        node, fromNode = key.split(",", 1)
        if(node == self.curNode):
            self.neighbor.append(fromNode)
        else:
            if self.curNode!="":
                yield self.curNode, " ".join(self.neighbor)
            self.neighbor=[]
            self.neighbor.append(fromNode)
            self.curNode=node
            
    def reducer1(self, key, values):
        node, fromNode = key.split(",", 1)
            
    def reducer_final(self):
    	# Do not forget the last node
        yield self.curNode, " ".join(self.neighbor) 

    SORT_VALUES = True

    JOBCONF = {
      'mapreduce.map.output.key.field.separator': ',',
      'mapreduce.job.reduces':2,
      'mapreduce.partition.keypartitioner.options':'-k1,1',
      'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
      'mapreduce.partition.keycomparator.options':'-k1,1n -k2,2n'
    }


if __name__ == '__main__':
    ReverseEdgeDirection.run()
