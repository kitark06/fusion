package com.kartikiyer.fusion.model;


public class MedicineOrders
{
	String	pcn;
	String	medName;
	String	dosageForm;
	String	labelerName;
	String	substanceName;
	String	medDesc;
	long		medPrice;

	

	public String getPcn()
	{
		return pcn;
	}

	public void setPcn(String pcn)
	{
		this.pcn = pcn;
	}

	public String getMedName()
	{
		return medName;
	}

	public void setMedName(String medName)
	{
		this.medName = medName;
	}

	public String getDosageForm()
	{
		return dosageForm;
	}

	public void setDosageForm(String dosageForm)
	{
		this.dosageForm = dosageForm;
	}

	public String getLabelerName()
	{
		return labelerName;
	}

	public void setLabelerName(String labelerName)
	{
		this.labelerName = labelerName;
	}

	public String getSubstanceName()
	{
		return substanceName;
	}

	public void setSubstanceName(String substanceName)
	{
		this.substanceName = substanceName;
	}

	public String getMedDesc()
	{
		return medDesc;
	}

	public void setMedDesc(String medDesc)
	{
		this.medDesc = medDesc;
	}

	public long getMedPrice()
	{
		return medPrice;
	}

	public void setMedPrice(long medPrice)
	{
		this.medPrice = medPrice;
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
		if (!(obj instanceof MedicineOrders))
			return false;
		MedicineOrders other = (MedicineOrders) obj;
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
		return "MedicineOrders [pcn=" + pcn + ", medName=" + medName + ", dosageForm=" + dosageForm + ", labelerName=" + labelerName + ", substanceName=" + substanceName + ", medDesc=" + medDesc
					+ ", medPrice=" + medPrice + "]";
	}


}
