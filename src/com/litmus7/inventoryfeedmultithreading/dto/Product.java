package com.litmus7.inventoryfeedmultithreading.dto;

public class Product {
	private int sku ;
	private String productName ;
	private double quantity ;
	private double price ;
	
	public int getSku() {
		return sku;
	}
	public void setSku(int sku) {
		this.sku = sku;
	}
	public String getProductName() {
		return productName;
	}
	public void setProductName(String productName) {
		this.productName = productName;
	}
	public double getQuantity() {
		return quantity;
	}
	public void setQuantity(double quantity) {
		this.quantity = quantity;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public String toDetailedString() {
        return 
               "Sku" + this.getSku() +
               ", product name ='" + this.getProductName() + '\'' 
               ;
    }
	

}
