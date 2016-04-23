package com.project.wordcount;

import com.mr.io.InCollector;
import com.mr.io.OutCollector;
import com.mr.io.Tuple;
import com.mr.mapreduce.Mapper;
import com.mr.mapreduce.Reducer;
import com.mr.net.App;

public class WordCount {
	public static void main(String[] args) throws Exception {
		App<String,String> w = 
				new App<String,String>( // Initialization
						new Map(), // Mapper
						new Reduce() // Reducer
						);
		// Run the workflow; send results to the InCollector
		w.run();
	}
	public static class Map implements Mapper<String, String>{
		private int m_minLetters = 1;

		/**
		 * Constructs a mapper and sets the minimum number of letters
		 * required to retain a tuple. This number is the <i>k</i> in the description
		 * above.
		 * @param k Minimum number of letters in the key to output a tuple
		 */

		@Override
		public void map(OutCollector<String,String> out, Tuple<String,String> t)
		{
			String[] words = t.getValue().split(" ");
			for (String w : words)
			{
				// Remove punctuation and convert to lowercase
//				System.out.print(w+"\t");
				String new_w = w.toLowerCase();
				new_w = new_w.replaceAll("[^\\w]", "");
				if (new_w.length() >= m_minLetters)
					out.collect(new Tuple<String,String>(new_w, "1"));
			}
		}
	}

	private static class Reduce implements Reducer<String,String>
	{
		private int m_numOccurrences = 2;
		
		@Override
		public void reduce(OutCollector<String,String> out, String key, InCollector<String,String> in)
		{
			int num_words = in.count();
			System.out.println("count received inside reducer ::: " + in.count());
			if (num_words >= m_numOccurrences)
				out.collect(new Tuple<String,String>(key, "" + num_words));
		}
	}


}

