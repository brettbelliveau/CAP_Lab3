import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

public class ExtractTitlesLinks {

	Document dom;
	
	public ExtractTitlesLinks(String text){
		try{
			dom = DocumentHelper.parseText(text);
		}
		catch (DocumentException e){
		e.printStackTrace();
		}
	}
	
	public String extractTitle(){
		String title = dom.selectSingleNode("/page/title").getText();
		return title;
	}
	
	public String extractBody(){
		String body = dom.selectSingleNode("/page//text").getText();
		return body;
	}
	
}
