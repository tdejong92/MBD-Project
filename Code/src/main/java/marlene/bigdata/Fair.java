package marlene.bigdata;

import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * Class that represents a specific fair
 * @author Tanja de Jong & Marlene Hol
 */
public class Fair {
	
	// The name of the fair
	public Text name;
	// The list of the hashtags used for this fair
	public List<Text> hashtags;
	// The account name of the fair. 
	public Text account;
	
	/**
	 * Constructor of the Fair-class that initializes the variables of the class
	 * @param name, the name of the fair
	 * @param hashtagsList, the hashtags used for the fair
	 * @param account, the account name of the fair. 
	 */
	public Fair(Text name, List<Text> hashtagsList, Text account) {
		this.name = name;
		this.hashtags = hashtagsList;
		this.account = account;
	}

}
