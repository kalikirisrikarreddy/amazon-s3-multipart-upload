package amazon.s3.multipartupload;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "firstName", "lastName", "profession", "favoriteChuckNorrisFact" })
public class Person {

	private String firstName;
	private String lastName;
	private String profession;
	private String favoriteChuckNorrisFact;

	public Person(String firstName, String lastName, String profession, String favoriteChuckNorrisFact) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.profession = profession;
		this.favoriteChuckNorrisFact = favoriteChuckNorrisFact;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getProfession() {
		return profession;
	}

	public void setProfession(String profession) {
		this.profession = profession;
	}

	public String getFavoriteChuckNorrisFact() {
		return favoriteChuckNorrisFact;
	}

	public void setFavoriteChuckNorrisFact(String favoriteChuckNorrisFact) {
		this.favoriteChuckNorrisFact = favoriteChuckNorrisFact;
	}

}
