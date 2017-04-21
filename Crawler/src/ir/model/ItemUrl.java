package ir.model;

import java.net.URL;

/**
 * 
 * A POJO to keep together an URL with its depth
 * 
 * @author Gabriel
 * 
 */

public class ItemUrl {
	
	URL url; 
	int srcId;
	int depth;
	
	public ItemUrl(URL url, int srcId, int depth){
		this.url=url;
		this.srcId= srcId;
		this.depth=depth;
	}
	
	public URL getUrl() {
		return url;
	}
	
	public void setUrl(URL url) {
		this.url = url;
	}
	
	public int getSrcId() {
		return srcId;
	}
	
	public void setSrcId(int srcId) {
		this.srcId= srcId;
	}
	
	public int getDepth(){
		return depth;
	}
	
	public void setDepth(int depth) {
		this.depth = depth;
	}
}
