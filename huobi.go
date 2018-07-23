package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
	"strings"
)

type SubStruct struct {
	Sub   string `json:"sub"`
	Id    string `json:"id"`
	Unsub string `json:"unsub"`
}

type Connection struct {
	conn *websocket.Conn
}

type Data struct {
	Symbols []Symbol `json:"data"`
}

type Symbol struct {
	Symbol string `json:"symbol"`
}

type Glass struct {
	Symbol string
	Tick   Tick `json:"tick"`
}

type Tick struct {
	Asks []interface{} `json:"asks"`
	Bids []interface{} `json:"bids"`
}

func (c *Connection) DialSocket(url string) *Connection {
	var ws websocket.Dialer
	wsConn, _, err := ws.Dial(url, nil)
	if err != nil {
		log.Println(err)
	}
	c.conn = wsConn
	return c
}

func (c *Connection) ReadLoop(symbol string) {
	subStruct := new(SubStruct)
	subStruct.Id = strconv.Itoa(time.Now().Nanosecond())
	subStruct.Sub = fmt.Sprintf("market.%s.depth.step1", symbol)
	reqBytes, err := json.Marshal(subStruct)
	if err != nil {
		log.Println(err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, reqBytes); err != nil {
		log.Println(err)
	}

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			fmt.Println(err)
		}

		res := UnGzip(message)

		jsonMap := make(map[string]interface{})

		if err = json.Unmarshal(res, &jsonMap); err == nil {

			if v, ok := jsonMap["ping"]; ok {
				pingMap := make(map[string]interface{})
				pingMap["pong"] = v
				pingParams, err := json.Marshal(pingMap)

				if err != nil {
					log.Println(err)
				}

				if err := c.conn.WriteMessage(websocket.TextMessage, pingParams); err != nil {
					log.Println(err)
				}
			}
		} else {
			log.Println(err)
		}

		if  _, ok := jsonMap["tick"]; ok  {
			glass := new(Glass)
			glass.Symbol = strings.Split(jsonMap["ch"].(string), ".")[1]
			if err = json.Unmarshal(res, glass); err != nil {
				log.Println(err)
			}
			Cached(glass)
		}
		return
	}
}

func UnGzip(byte []byte) []byte {
	b := bytes.NewBuffer(byte)
	r, _ := gzip.NewReader(b)
	defer r.Close()
	undatas, err := ioutil.ReadAll(r)
	if err != nil {
		log.Println(err)
	}
	return undatas
}

func GetAllSymbols() *Data {
	url := "https://api.huobi.pro/v1/common/symbols"
	resp, err := http.Get(url)
	if err != nil {
		log.Println(err)
	}
	defer resp.Body.Close()

	symbols, err := ioutil.ReadAll(resp.Body)
	data := new(Data)
	if err = json.Unmarshal(symbols, data); err != nil {
		log.Println(err)
	}
	return data
}

func Cached(glass *Glass) {
	mc := memcache.New("127.0.0.1:11211")
	asks_bids := map[string]interface{}{"asks": glass.Tick.Asks, "bids": glass.Tick.Bids}
	b, err := json.Marshal(asks_bids)
	if err != nil {
		log.Println(err)
	}
	mc.Set(&memcache.Item{Key: "huobi_" + glass.Symbol, Value: b})
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
	for _, s := range allSymbols {
		symbols = append(symbols, "huobi_" + s.Symbol)
	}
	ticks, err := json.Marshal(GetCache(symbols))
	if err != nil {
		log.Println(err)
	}
	fmt.Fprint(w, string(ticks))
}

func main() {
	url := "wss://api.huobi.pro/ws/"
	conn := &Connection{}
	c := conn.DialSocket(url)
	go func() {
		for {
			for _, s := range GetAllSymbols().Symbols {
				c.ReadLoop(s.Symbol)
			}
			time.Sleep(3 * time.Second)
		}
	}()
	http.HandleFunc("/", CachedHandler)
	http.ListenAndServe(":8080", nil)
}
