package com.kartikiyer.fusion.model;


public class EnrichedData
{
	String			pcn;

	PatientInfo		patientInfo;
	Treatment			treatment;
	MedicineOrders		medicineOrders;
	BillingCost		billingCost;
	InsuranceDetails	insuranceDetails;

	public String getPcn()
	{
		return pcn;
	}

	public void setPcn(String pcn)
	{
		this.pcn = pcn;
	}

	public PatientInfo getPatientInfo()
	{
		return patientInfo;
	}

	public void setPatientInfo(PatientInfo patientInfo)
	{
		this.patientInfo = patientInfo;
	}

	public Treatment getTreatment()
	{
		return treatment;
	}

	public void setTreatment(Treatment treatment)
	{
		this.treatment = treatment;
	}

	public MedicineOrders getMedicineOrders()
	{
		return medicineOrders;
	}

	public void setMedicineOrders(MedicineOrders medicineOrders)
	{
		this.medicineOrders = medicineOrders;
	}

	public BillingCost getBillingCost()
	{
		return billingCost;
	}

	public void setBillingCost(BillingCost billingCost)
	{
		this.billingCost = billingCost;
	}

	public InsuranceDetails getInsuranceDetails()
	{
		return insuranceDetails;
	}

	public void setInsuranceDetails(InsuranceDetails insuranceDetails)
	{
		this.insuranceDetails = insuranceDetails;
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
		if (!(obj instanceof EnrichedData))
			return false;
		EnrichedData other = (EnrichedData) obj;
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
		return "EnrichedData [pcn=" + pcn + ", patientInfo=" + patientInfo + ", treatment=" + treatment + ", medicineOrders=" + medicineOrders + ", billingCost=" + billingCost
					+ ", insuranceDetails=" + insuranceDetails + "]";
	}


}
