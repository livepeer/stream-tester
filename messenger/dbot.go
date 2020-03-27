package messenger

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Necroforger/dgrouter/exrouter"
	"github.com/bwmarrin/discordgo"
	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/broadcaster"
	"github.com/livepeer/stream-tester/apis/livepeer"
)

const botPrefix = "!st "
const internalACDomain = ".tenant-livepeer.svc.cluster.local"

type (
	discordBot struct {
		ctx       context.Context
		sess      *discordgo.Session
		channelID string
		lapiToken string
	}
)

func startBot(ctx context.Context, botToken, channelID, lapiToken string) {
	sess, err := discordgo.New("Bot " + botToken)
	glog.Infof("bog token is '%s'", botToken)
	if err != nil {
		panic(err)
	}

	bot := discordBot{
		ctx:       ctx,
		sess:      sess,
		channelID: channelID,
		lapiToken: lapiToken,
	}
	bot.init()
	bot.start()
}

func (bot *discordBot) init() {
	router := bot.setupRouter()
	// bot.sess.LogLevel = 1000
	// Register the messageCreate func as a callback for MessageCreate events.
	// bot.AddHandler(messageCreate)
	// Register ready as a callback for the ready events.
	bot.sess.AddHandler(ready)
	// Add message handler
	bot.sess.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageCreate) {
		// todo check for channel (allow to receive only direct messages and in specified channel)
		if m.GuildID != "" && m.ChannelID != bot.channelID {
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
	router.On("stats", bot.stats)

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

func (bot *discordBot) bstreams(ctx *exrouter.Context) {
	ctx.Reply()
	// ctx.Reply("bstreams called with arguments:\n", strings.Join(ctx.Args, ";"))
	ctx.Reply("Getting streams...")
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
