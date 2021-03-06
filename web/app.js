var address = "http://127.0.0.1:6060";
var search = new Vue({
	el: '#search',
	data: {
		message: 'Hello Vue!'
	}
});
document.addEventListener('DOMContentLoaded', function () {
	var elems = document.querySelectorAll('.collapsible');
	var instances = M.Collapsible.init(elems, {}/*options*/);

	jQuery.post(address, JSON.stringify({method: "keywords", params: {}, jsonrpc: "2.0", id: Date.now()}), item => {
		keywords.keywords = JSON.parse(item).result;
	});
});

var newsSearch = new Vue({
	el: '#news_search',
	data: {
		text: "",
	},
	methods: {}
});

var normalSearch = new Vue({
	el: '#normal_search',
	data: {
		text: "",
	},
	methods: {}
});

var result = new Vue({
	el: '#result',
	data: {res: []},
	methods: {}
});

var keywords = new Vue({
	el: '#keywords',
	data: {keywords: []},
	methods: {
		searchNews: function (query) {
			method = "newsSearch";
			jQuery.post(address, JSON.stringify({method, params: {query}, jsonrpc: "2.0", id: Date.now()}), item => {
				result.res = JSON.parse(item).result;
			});
		}

	}
});

var advanceSearch = new Vue({
	el: '#advance_search',
	data: {
		must: [{value: ""}],
		must_not: [{value: ""}],
		should: [{value: ""}],
	},
	methods: {
		add_must: function () {
			this.must.push({value: ""});
		},
		add_must_not: function () {
			this.must_not.push({value: ""});
		},
		add_should: function () {
			this.should.push({value: ""});
		}
	}
});


var search_button = new Vue({
	el: '#search_button',
	methods: {
		click: function () {
			let params, method;
			let safety = document.getElementById('safety').checked;
			let pageRank = document.getElementById('pageRank').checked;

			if (document.getElementsByClassName("active").item(0).getAttribute('id') === 'advance_search') {
				params = {
					must: advanceSearch.must.map(item => item.value),
					must_not: advanceSearch.must_not.map(item => item.value),
					should: advanceSearch.should.map(item => item.value),
					safety, pageRank
				};
				method = "advanceSearch";
			}
			else if (document.getElementsByClassName("active").item(0).getAttribute('id') === 'normal_search') {

				params = {
					text: normalSearch.text,
					safety, pageRank
				};
				method = "normalSearch";
			}
			else {
				params = {
					query: newsSearch.text
				};
				method = "newsSearch";
			}

			console.log(method, params);
			jQuery.post(address, JSON.stringify({method, params, jsonrpc: "2.0", id: Date.now()}), item => {
				result.res = JSON.parse(item).result;
			});
		}
	}
});
