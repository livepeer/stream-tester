package messenger

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Necroforger/dgrouter/exrouter"
	"github.com/bwmarrin/discordgo"
	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/apis/broadcaster"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/internal/codec"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
)

const botPrefix = "!st "
const internalACDomain = ".tenant-livepeer.svc.cluster.local"

type (
	discordBot struct {
		ctx       context.Context
		sess      *discordgo.Session
		channelID []string
		lapiToken string
		router    *exrouter.Route
	}
)

// httpTimeout http timeout downloading manifests/segments
const httpTimeout = 16 * time.Second

var (
	mhttpClient = &http.Client{
		// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
		// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		// Transport: &http2.Transport{AllowHTTP: true},
		Timeout: 4 * time.Second,
	}

	httpClient = &http.Client{
		// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
		// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		// Transport: &http2.Transport{AllowHTTP: true},
		Timeout: httpTimeout,
	}
	gbot *discordBot
)

// AddBotCommand registers a handler function
func AddBotCommand(name string, handler exrouter.HandlerFunc) *exrouter.Route {
	if gbot != nil {
		return gbot.router.On(name, handler)
	}
	return nil
}

func startBot(ctx context.Context, botToken, channelID, lapiToken string) {
	sess, err := discordgo.New("Bot " + botToken)
	glog.Infof("bog token is '%s'", botToken)
	if err != nil {
		panic(err)
	}

	bot := discordBot{
		ctx:       ctx,
		sess:      sess,
		channelID: strings.Split(channelID, ","),
		lapiToken: lapiToken,
	}
	gbot = &bot
	bot.init()
	bot.start()
}

func (bot *discordBot) init() {
	router := bot.setupRouter()
	bot.router = router
	bot.sess.LogLevel = 1000
	// Register the messageCreate func as a callback for MessageCreate events.
	// bot.AddHandler(messageCreate)
	// Register ready as a callback for the ready events.
	bot.sess.AddHandler(ready)
	// Add message handler
	bot.sess.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageCreate) {
		// todo check for channel (allow to receive only direct messages and in specified channel)
		// glog.Infof("===> mess: %s", m.Content)
		if m.GuildID != "" && len(bot.channelID) > 0 && !utils.StringsSliceContains(bot.channelID, m.ChannelID) {
			// ignore messages not in specified channel
			return
		}
		router.FindAndExecute(bot.sess, botPrefix, bot.sess.State.User.ID, m.Message)
	})
}

// blocks
func (bot *discordBot) start() {
	// Open a websocket connection to Discord and begin listening.
	err := bot.sess.Open()
	if err != nil {
		glog.Errorf("Error opening connection to Discord %v", err)
		return
	}
	glog.Infoln("Bot is now running.")

	// wait for the global exit
	<-bot.ctx.Done()
	bot.sess.UpdateStatus(255, "")

	// Cleanly close down the Discord session.
	bot.sess.Close()
	glog.Infof("Bot stopped")
}

// This function will be called (due to AddHandler above) every time a new
// message is created on any channel that the autenticated bot has access to.
func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {

	// selfMessage := m.Author.ID == s.State.User.ID
	// glog.Infof("got self=%v message %+m", selfMessage, m)
	mg := ""
	if m.Member != nil {
		mg = m.Member.GuildID
	}
	glog.Infof("Channl id %s eq user id %v (my user id %s) mess guild %s member guild %s", m.ChannelID, m.ChannelID == s.State.User.ID, s.State.User.ID,
		m.GuildID, mg)

	// Ignore all messages created by the bot itself
	// This isn't required in this specific example but it's a good practice.
	if m.Author.ID == s.State.User.ID {
		return
	}
	isDirectMessage := m.GuildID == ""
	if !isDirectMessage {
		return
	}

	// If the message is "ping" reply with "Pong!"
	if m.Content == "ping" {
		s.ChannelMessageSend(m.ChannelID, "Pong!")
	}

	// If the message is "pong" reply with "Ping!"
	if m.Content == "pong" {
		s.ChannelMessageSend(m.ChannelID, "Ping!")
	}
}

// This function will be called (due to AddHandler above) when the bot receives
// the "ready" event from Discord.
func ready(s *discordgo.Session, event *discordgo.Ready) {

	// Set the playing status.
	s.UpdateStatus(0, botPrefix)
}

func (bot *discordBot) setupRouter() *exrouter.Route {
	router := exrouter.New()

	/*
		router.On("sub", nil).
			On("sub2", func(ctx *exrouter.Context) {
				ctx.Reply("sub2 called with arguments:\n", strings.Join(ctx.Args, ";"))
			}).
			On("sub3", func(ctx *exrouter.Context) {
				ctx.Reply("sub3 called with arguments:\n", strings.Join(ctx.Args, ";"))
			})
	*/
	router.On("bstreams", bot.bstreams).Desc("Show active streams across all broadcasters available through API in AC")
	// router.On("stats", bot.stats)
	// router.On("img", bot.imgTest)

	router.Default = router.On("help", func(ctx *exrouter.Context) {
		var f func(depth int, r *exrouter.Route) string
		f = func(depth int, r *exrouter.Route) string {
			text := ""
			for _, v := range r.Routes {
				text += strings.Repeat("  ", depth) + v.Name + " : " + v.Description + "\n"
				text += f(depth+1, &exrouter.Route{Route: v})
			}
			return text
		}
		ctx.Reply("```" + f(0, router) + "```")
	}).Desc("prints this help menu")
	return router
}

func (bot *discordBot) imgTest(ctx *exrouter.Context) {
	ctx.Reply("Hello")
	// ctx.Ses.ChannelMessageSendComplex(ctx.Msg.ChannelID, &discordgo.MessageSend{discordgo.File: &discordgo.File{Name: name, Reader: r}})
	name := "name01.jpg"
	contType := "image/jpeg"
	img := codec.TSFirstImage(nil)
	if img == nil {
		return
	}
	jpg := utils.Img2Jpeg(img)
	r := bytes.NewReader(jpg)
	ctx.Ses.ChannelMessageSendComplex(ctx.Msg.ChannelID, &discordgo.MessageSend{
		File: &discordgo.File{
			Name:        name,
			ContentType: contType,
			Reader:      r,
		},
	})
}

type streamDesc struct {
	broadcaster string
	mid         string
	exturl      string
	inturl      string
	jpg         []byte
}

func (bot *discordBot) bstreamsAsync(ctx *exrouter.Context, withImages bool) {
	ctx.Reply(fmt.Sprintf("Getting images... (with images %v)", withImages))
	lapi := livepeer.NewLivepeer(bot.lapiToken, livepeer.ACServer, nil) // hardcode AC server for now
	lapi.Init()
	bds, err := lapi.Broadcasters()
	if err != nil {
		glog.Error(err)
		ctx.Reply(err.Error())
		return
	}
	glog.Infof("Got broadcasters to use: %v", bds)
	unique := make(map[string]*streamDesc)
	rc := make(chan *streamDesc, 16)
	totc := make(chan int, 16)
	for _, bs := range bds {
		go bot.getStreams(ctx, rc, totc, withImages, bs)
	}
	cx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	var total, gotStreams, gotbn int
out:
	for {
		select {
		case <-cx.Done():
			break out
		case nt := <-totc:
			gotbn++
			total += nt
		case sd := <-rc:
			unique[sd.mid] = sd
			// ctx.Reply(fmt.Sprintf("Got link: %s", sd.exturl))
			gotStreams++
			if len(sd.jpg) > 0 {
				name := fmt.Sprintf("%s_%s.jpg", sd.broadcaster, sd.mid)
				contType := "image/jpeg"
				r := bytes.NewReader(sd.jpg)
				ctx.Ses.ChannelMessageSendComplex(ctx.Msg.ChannelID, &discordgo.MessageSend{
					Content: sd.exturl,
					File: &discordgo.File{
						Name:        name,
						ContentType: contType,
						Reader:      r,
					},
				})
			}
		}
		if gotbn == len(bds) && gotStreams >= total {
			break
		}
	}
	cancel()
}

func getBName(burl string) string {
	pu, _ := url.Parse(burl)
	p1s := strings.Split(pu.Hostname(), ".")
	p2s := strings.Split(p1s[0], "-")
	return p2s[len(p2s)-1]
}

func (bot *discordBot) getStreams(ctx *exrouter.Context, rc chan *streamDesc, totalc chan int, withImages bool, bexturl string) {
	binturl := burlExt2Int(bexturl)
	glog.Infof("Ext url %s int url %s", bexturl, binturl)
	// binturl = "http://localhost"
	furl := binturl + ":7935/status"
	st, err := broadcaster.Status(furl)
	if err != nil {
		emsg := fmt.Sprintf("Can't get status from broadcaster url=%s err=%v", furl, err)
		glog.Error(emsg)
		ctx.Reply(emsg)
		totalc <- 0
		return
	}
	bname := getBName(bexturl)
	emsg := fmt.Sprintf("Broadcaster %s has %d active streams", bexturl, len(st.Manifests))
	ctx.Reply(emsg)
	totalc <- len(st.Manifests)
	if len(st.Manifests) > 0 {
		links := make([]string, 0, len(st.Manifests))
		for mid := range st.Manifests {
			link := fmt.Sprintf("%s/stream/%s.m3u8", bexturl, mid)
			links = append(links, link)
			sd := &streamDesc{
				broadcaster: bname,
				mid:         mid,
				exturl:      link,
				// inturl:      fmt.Sprintf("%s/stream/%s.m3u8", binturl, mid),
				inturl: fmt.Sprintf("%s:8935/stream/%s.m3u8", binturl, mid),
			}
			if withImages || true {
				go bot.getStreamImage(rc, sd)
			} else {
				rc <- sd
			}
			// ctx.Ses.ChannelMessageSendEmbed(ctx.Msg.ChannelID, &discordgo.MessageEmbed{
			// 	Description: "video " + mid,
			// 	Video: &discordgo.MessageEmbedVideo{
			// 		URL: link,
			// 	},
			// })
		}
		ctx.Reply(strings.Join(links, "\n"))
	}
}

func (bot *discordBot) getStreamImage(rc chan *streamDesc, sd *streamDesc) {
	defer func() {
		rc <- sd
	}()

	mpl, err := utils.DownloadMasterPlaylist(sd.inturl)
	if err != nil || len(mpl.Variants) < 1 {
		return
	}
	masterURI, _ := url.Parse(sd.inturl)
	pvrui, err := url.Parse(mpl.Variants[0].URI)
	if err != nil {
		glog.Error(err)
		return
	}
	// glog.Infof("Parsed uri: %+v", pvrui, pvrui.IsAbs)
	if !pvrui.IsAbs() {
		pvrui = masterURI.ResolveReference(pvrui)
	}
	muri := pvrui.String()
	resp, err := mhttpClient.Do(uhttp.GetRequest(muri))
	if err != nil {
		glog.Infof("===== get error getting media playlist %s: %v", muri, err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Errorf("Status error getting media playlist %s: %v (%s) body: %s", muri, resp.StatusCode, resp.Status, string(b))
		return
	}
	mdpl, _ := m3u8.NewMediaPlaylist(100, 100)
	err = mdpl.DecodeFrom(resp.Body, true)
	// err = mpl.Decode(*bytes.NewBuffer(b), true)
	resp.Body.Close()
	if err != nil {
		glog.Infof("Error getting media playlist uri=%s err=%v", muri, err)
		return
	}
	if mdpl.Count() == 0 {
		return
	}
	segURI := mdpl.Segments[mdpl.Count()-1].URI
	purl, err := url.Parse(segURI)
	if err != nil {
		glog.Error(err)
		return
	}
	mplu, _ := url.Parse(muri)
	if !purl.IsAbs() {
		segURI = mplu.ResolveReference(purl).String()
	}
	resp, err = httpClient.Do(uhttp.GetRequest(segURI))
	if err != nil {
		glog.Infof("Error downloading video segment %s: %v", segURI, err)
		return
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		glog.Errorf("Status error downloading media segment %s: %v (%s) body: %s", segURI, resp.StatusCode, resp.Status, string(b))
		return
	}
	if err != nil {
		glog.Errorf("Error downloading first segment uri=%s err=%v", segURI, err)
		return
	}
	glog.Infof("Downloaded segment %s", segURI)
	img := codec.TSFirstImage(b)
	if img != nil {
		sd.jpg = utils.Img2Jpeg(img)
	}
}

func (bot *discordBot) bstreams(ctx *exrouter.Context) {
	ctx.Reply("bstreams called with arguments:\n", strings.Join(ctx.Args, ";"))
	ctx.Reply("Getting streams...")
	if len(ctx.Args) > 1 && ctx.Args[1] == "img" {
		bot.bstreamsAsync(ctx, true)
		return
	}
	lapi := livepeer.NewLivepeer(bot.lapiToken, livepeer.ACServer, nil) // hardcode AC server for now
	lapi.Init()
	bds, err := lapi.Broadcasters()
	if err != nil {
		glog.Error(err)
		ctx.Reply(err.Error())
		return
	}
	glog.Infof("Got broadcasters to use: %v", bds)
	unique := make(map[string]string)
	for _, bexturl := range bds {
		binturl := burlExt2Int(bexturl)
		glog.Infof("Ext url %s int url %s", bexturl, binturl)
		// binturl = "http://localhost"
		furl := binturl + ":7935/status"
		st, err := broadcaster.Status(furl)
		if err != nil {
			emsg := fmt.Sprintf("Can't get status from broadcaster url=%s err=%v", furl, err)
			glog.Error(emsg)
			ctx.Reply(emsg)
			return
		}
		emsg := fmt.Sprintf("Broadcaster %s has %d active streams", bexturl, len(st.Manifests))
		ctx.Reply(emsg)
		if len(st.Manifests) > 0 {
			links := make([]string, 0, len(st.Manifests))
			for mid := range st.Manifests {
				link := fmt.Sprintf("%s/stream/%s.m3u8", bexturl, mid)
				unique[mid] = link
				links = append(links, link)
				// ctx.Ses.ChannelMessageSendEmbed(ctx.Msg.ChannelID, &discordgo.MessageEmbed{
				// 	Description: "video " + mid,
				// 	Video: &discordgo.MessageEmbedVideo{
				// 		URL: link,
				// 	},
				// })
			}
			ctx.Reply(strings.Join(links, "\n"))
		}
	}
	if len(unique) > 0 {
		links := make([]string, 0, len(unique))
		for _, link := range unique {
			links = append(links, link)
		}
		ctx.Reply(fmt.Sprintf("%d unique streams:\n%s", len(unique), strings.Join(links, "\n")))
	}
}

func burlExt2Int(uri string) string {
	pu, _ := url.Parse(uri)
	hp := strings.Split(pu.Hostname(), ".")
	return "http://" + hp[0] + internalACDomain
}

func (bot *discordBot) stats(ctx *exrouter.Context) {
	ctx.Reply("stats called with arguments:\n", strings.Join(ctx.Args, ";"))
}
