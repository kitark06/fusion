package com.kartikiyer.fusion.model;


public class BillingCost
{
	String	pcn;
	String	patientID;
	long		totalCost;

	public String getPcn()
	{
		return pcn;
	}

	public void setPcn(String pcn)
	{
		this.pcn = pcn;
	}

	public String getPatientID()
	{
		return patientID;
	}

	public void setPatientID(String patientID)
	{
		this.patientID = patientID;
	}

	public long getTotalCost()
	{
		return totalCost;
	}

	public void setTotalCost(long totalCost)
	{
		this.totalCost = totalCost;
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
		if (!(obj instanceof BillingCost))
			return false;
		BillingCost other = (BillingCost) obj;
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
		return "BillingCost [pcn=" + pcn + ", patientID=" + patientID + ", totalCost=" + totalCost + "]";
	}


}
