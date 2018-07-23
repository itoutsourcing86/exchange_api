package main

import (
	"encoding/json"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ExchangeInfo struct {
	Symbols map[string]interface{} `json:"result"`
}

type Glass struct {
	Symbol string
	Result map[string]Result `json:"result"`
	//Result map[string]map[string][][]interface{} `json:"result"`
}

type Result struct {
	Asks [][]interface{} `json:"asks"`
	Bids [][]interface{} `json:"bids"`
}

func GetAllSymbols() *ExchangeInfo {
	url := "https://api.kraken.com/0/public/AssetPairs"

	resp, err := http.Get(url)
	if err != nil {
		log.Println(err)
	}
	defer resp.Body.Close()

	symbols, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
	}

	res := new(ExchangeInfo)
	if err = json.Unmarshal(symbols, res); err != nil {
		log.Println(err)
	}
	return res
}

func GetOrderbook(symbol <-chan string) {
	for s := range symbol {
		url := fmt.Sprintf("https://api.kraken.com/0/public/Depth?pair=%s&count=10", s)
		resp, err := http.Get(url)
		if err != nil {
			log.Println(err)
		}
		defer resp.Body.Close()

		orderbook, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
		}

		res := new(Glass)
		if err = json.Unmarshal(orderbook, res); err != nil {
			log.Println(err)
		}
		Cached(res, s)
	}
}

func Cached(glass *Glass, symbol string) {
	mc := memcache.New("127.0.0.1:11211")
	var asks, bids [][]float64
	for _, v := range glass.Result[symbol].Asks {
		asksPrice, _ := strconv.ParseFloat(v[0].(string), 64)
		asksQuant, _ := strconv.ParseFloat(v[1].(string), 64)
		asks = append(asks, []float64{asksPrice, asksQuant})
	}
	for _, v := range glass.Result[symbol].Bids {
		bidsPrice, _ := strconv.ParseFloat(v[0].(string), 64)
		bidsQuant, _ := strconv.ParseFloat(v[1].(string), 64)
		bids = append(asks, []float64{bidsPrice, bidsQuant})
	}
	asks_bids := map[string][][]float64{"asks": asks, "bids": bids}
	b, err := json.Marshal(asks_bids)
	if err != nil {
		log.Println(err)
	}
	mc.Set(&memcache.Item{Key: "kraken_"+symbol, Value: b})
}

func GetCache(keys []string) map[string]string {
	mc := memcache.New("127.0.0.1:11211")
	it, err := mc.GetMulti(keys)
	if err != nil {
		log.Println(err)
	}
	res := make(map[string]string)
	for k, v := range it {
		res[k] = string(v.Value)
	}
	return res
}

func CachedHandler(w http.ResponseWriter, r *http.Request) {
	allSymbols := GetAllSymbols().Symbols
	var symbols []string
	for key := range allSymbols {
		if strings.Contains(key, ".d") {
			key = strings.Replace(key, ".d", "", 1)
		}
		symbols = append(symbols, "kraken_"+key)
	}
	ticks, err := json.Marshal(GetCache(symbols))
	if err != nil {
		log.Println(err)
	}
	fmt.Fprint(w, string(ticks))
}

func main() {
	symbols := GetAllSymbols()
	jobs := make(chan string, 100)

	go func() {
		for {
			for w := 0; w <= len(symbols.Symbols); w++ {
				go GetOrderbook(jobs)
			}

			for key := range symbols.Symbols {
				if strings.Contains(key, ".d") {
					key = strings.Replace(key, ".d", "", 1)
				}
				jobs <- key
			}
			time.Sleep(3 * time.Second)
		}
	}()
	http.HandleFunc("/", CachedHandler)
	http.ListenAndServe(":8080", nil)
}
