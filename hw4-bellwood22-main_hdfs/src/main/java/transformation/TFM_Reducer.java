package transformation;

public class TFM_Reducer {

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static final int numberOfPages = 10000*10000;
        private static final double alpha = 0.15;
        private int danglingCount = 0;

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double rank = 0.0;
            StringBuilder links = new StringBuilder();

            if (key.get()==-1) {
                danglingCount=0;
                for (Text value:values) {
                    danglingCount++;
                }
            } else {
                for (Text value : values) {
                    String content = value.toString();
                    if (content.startsWith("L")) {
                        if(links.length() > 0) links.append(",");
                        links.append(content.substring(1));
                    } else if (content.startsWith("R")) {
                        rank = Double.parseDouble(content.substring(1));
                    }
                }
            }

            // pr value
            double newRank=0.0;
            if (key.get()!=-1) {
                newRank = (1 - alpha) / (double) numberOfPages + alpha * ((1.0 / (double) numberOfPages) * danglingCount + rank);
            }
            context.write(key, new Text("New Rank: " + newRank));
            System.out.println("Page ID: " + key.get() + ", New Rank: " + newRank);



        }
    }

}
