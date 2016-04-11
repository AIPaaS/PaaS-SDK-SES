package com.ai.paas.ipaas.ses.doc.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/doc")
public class HelpController {
	
	@RequestMapping(value = "/")
	public String mapping() {
		return "/doc";
	}
	
}
