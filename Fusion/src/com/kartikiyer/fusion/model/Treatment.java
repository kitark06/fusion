package com.kartikiyer.fusion.model;


import java.util.Date;


public class Treatment
{
	String	pcn;
	String	icd;
	String	treatmentDesc;
	Date		visitDate;

	public String getIcd()
	{
		return icd;
	}

	public void setIcd(String icd)
	{
		this.icd = icd;
	}

	public String getTreatmentDesc()
	{
		return treatmentDesc;
	}

	public void setTreatmentDesc(String treatmentDesc)
	{
		this.treatmentDesc = treatmentDesc;
	}

	public Date getVisitDate()
	{
		return visitDate;
	}

	public void setVisitDate(Date visitDate)
	{
		this.visitDate = visitDate;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pcn == null) ? 0 : pcn.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Treatment))
			return false;
		Treatment other = (Treatment) obj;
		if (pcn == null)
		{
			if (other.pcn != null)
				return false;
		}
		else if (!pcn.equals(other.pcn))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return "Treatment [icd=" + icd + ", treatmentDesc=" + treatmentDesc + ", visitDate=" + visitDate + "]";
	}

}
