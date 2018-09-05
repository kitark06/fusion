package com.kartikiyer.fusion.model;


import java.util.Date;


public class InsuranceDetails
{
	String	pcn;
	String	providerName;
	Date		opened;
	String	reason;
	String	disposition;
	String	conclusion;
	long		amountPayed;
	Date		insuranceDate;

	public String getPcn()
	{
		return pcn;
	}

	public void setPcn(String pcn)
	{
		this.pcn = pcn;
	}

	public String getProviderName()
	{
		return providerName;
	}

	public void setProviderName(String providerName)
	{
		this.providerName = providerName;
	}

	public Date getOpened()
	{
		return opened;
	}

	public void setOpened(Date opened)
	{
		this.opened = opened;
	}

	public String getReason()
	{
		return reason;
	}

	public void setReason(String reason)
	{
		this.reason = reason;
	}

	public String getDisposition()
	{
		return disposition;
	}

	public void setDisposition(String disposition)
	{
		this.disposition = disposition;
	}

	public String getConclusion()
	{
		return conclusion;
	}

	public void setConclusion(String conclusion)
	{
		this.conclusion = conclusion;
	}

	public long getAmountPayed()
	{
		return amountPayed;
	}

	public void setAmountPayed(long amountPayed)
	{
		this.amountPayed = amountPayed;
	}

	public Date getInsuranceDate()
	{
		return insuranceDate;
	}

	public void setInsuranceDate(Date insuranceDate)
	{
		this.insuranceDate = insuranceDate;
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
		if (!(obj instanceof InsuranceDetails))
			return false;
		InsuranceDetails other = (InsuranceDetails) obj;
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
		return "InsuranceDetails [pcn=" + pcn + ", providerName=" + providerName + ", opened=" + opened + ", reason=" + reason + ", disposition=" + disposition + ", conclusion=" + conclusion
					+ ", amountPayed=" + amountPayed + ", insuranceDate=" + insuranceDate + "]";
	}


}
