package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 8 - Mapper
 */

class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				MonthIncome> {// Output value type

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String[] date = key.toString().split("-");
		String year = date[0];
		String monthID = date[1];

		Double income = Double.parseDouble(value.toString());

		MonthIncome monthIncome = new MonthIncome();

		monthIncome.setMonthID(monthID);
		monthIncome.setIncome(income);

		// emit the pair (year, (month,income))
		context.write(new Text(year), monthIncome);
	}
}
