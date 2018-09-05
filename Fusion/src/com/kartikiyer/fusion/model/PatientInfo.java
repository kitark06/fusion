package com.kartikiyer.fusion.model;


public class PatientInfo
{
	String	patientID;
	String	patientname;
	int		age;
	String	streetAddress;
	String	city;
	String	email;
	String	number;

	public String getPatientID()
	{
		return patientID;
	}

	public void setPatientID(String patientID)
	{
		this.patientID = patientID;
	}

	public String getPatientname()
	{
		return patientname;
	}

	public void setPatientname(String patientname)
	{
		this.patientname = patientname;
	}

	public int getAge()
	{
		return age;
	}

	public void setAge(int age)
	{
		this.age = age;
	}

	public String getStreetAddress()
	{
		return streetAddress;
	}

	public void setStreetAddress(String streetAddress)
	{
		this.streetAddress = streetAddress;
	}

	public String getCity()
	{
		return city;
	}

	public void setCity(String city)
	{
		this.city = city;
	}

	public String getEmail()
	{
		return email;
	}

	public void setEmail(String email)
	{
		this.email = email;
	}

	public String getNumber()
	{
		return number;
	}

	public void setNumber(String number)
	{
		this.number = number;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((patientID == null) ? 0 : patientID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof PatientInfo))
			return false;
		PatientInfo other = (PatientInfo) obj;
		if (patientID == null)
		{
			if (other.patientID != null)
				return false;
		}
		else if (!patientID.equals(other.patientID))
			return false;
		return true;
	}
	

	@Override
	public String toString()
	{
		return "PatientInfo [patientID=" + patientID + ", patientname=" + patientname + ", age=" + age + ", streetAddress=" + streetAddress + ", city=" + city + ", email=" + email + ", number="
					+ number + "]";
	}

}
