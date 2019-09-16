<!DOCTYPE html>


    <html itemscope itemtype="http://schema.org/QAPage" class="html__responsive">

    <head>

        <title>.net - {{.Title}}</title>
        <link rel="shortcut icon" href="https://cdn.sstatic.net/Sites/stackoverflow/img/favicon.ico?v=4f32ecc8f43d">
        <link rel="apple-touch-icon image_src" href="https://cdn.sstatic.net/Sites/stackoverflow/img/apple-touch-icon.png?v=c78bd457575a">
        <link rel="search" type="application/opensearchdescription+xml" title="Stack Overflow" href="/opensearch.xml">
        <meta name="viewport" content="width=device-width, height=device-height, initial-scale=1.0, minimum-scale=1.0">


        <meta property="og:type" content= "website" />
        <meta property="og:url" content="https://stackoverflow.com/questions/9/how-do-i-calculate-someones-age-in-c"/>
        <meta property="og:site_name" content="Stack Overflow" />
        <meta property="og:image" itemprop="image primaryImageOfPage" content="https://cdn.sstatic.net/Sites/stackoverflow/img/apple-touch-icon@2.png?v=73d79a89bded" />
        <meta name="twitter:card" content="summary"/>
        <meta name="twitter:domain" content="stackoverflow.com"/>
        <meta name="twitter:title" property="og:title" itemprop="name" content="{{.Title}}" />
        
    <link rel="stylesheet" type="text/css" href="https://cdn.sstatic.net/Shared/stacks.css?v=6a897dbbc53b" >
    <link rel="stylesheet" type="text/css" href="https://cdn.sstatic.net/Sites/stackoverflow/primary.css?v=0804c1305a25" >

    
            <meta name="twitter:app:country" content="US" />
            <meta name="twitter:app:name:iphone" content="Stack Exchange iOS" />
            <meta name="twitter:app:id:iphone" content="871299723" />
            <meta name="twitter:app:url:iphone" content="se-zaphod://stackoverflow.com/questions/9/how-do-i-calculate-someones-age-in-c" />
            <meta name="twitter:app:name:ipad" content="Stack Exchange iOS" />
            <meta name="twitter:app:id:ipad" content="871299723" />
            <meta name="twitter:app:url:ipad" content="se-zaphod://stackoverflow.com/questions/9/how-do-i-calculate-someones-age-in-c" />
            <meta name="twitter:app:name:googleplay" content="Stack Exchange Android">
            <meta name="twitter:app:url:googleplay" content="http://stackoverflow.com/questions/9/how-do-i-calculate-someones-age-in-c">
            <meta name="twitter:app:id:googleplay" content="com.stackexchange.marvin">
       


    </head>
        <body class="question-page unified-theme">
    <div id="notify-container"></div>
    <div id="custom-header"></div>




<header class="top-bar js-top-bar top-bar__network _fixed">
    <div class="-container">
        <div class="-main">
                <a href="#" class="left-sidebar-toggle p0 ai-center jc-center js-left-sidebar-toggle"><span class="ps-relative"></span></a>
                                <a href="https://stackoverflow.com" class="-logo">
                        <span class="-img _glyph">Stack Overflow</span>
                    </a>



        </div>

            <form id="search" action=/search method="get" class="searchbar js-searchbar " autocomplete="off" role="search">
                    <div class="ps-relative">
                        <input name="q" type="text" placeholder="Search…" value="" autocomplete="off" maxlength="240" class="s-input js-search-field " />
                        <button type="submit" aria-label="Search…" class="s-btn s-btn__primary s-btn__icon btn-topbar-primary js-search-submit"><svg aria-hidden="true" class="svg-icon iconSearch" width="18" height="18" viewBox="0 0 18 18"><path d="M12.86 11.32L18 16.5 16.5 18l-5.18-5.14v-.35a7 7 0 1 1 1.19-1.19h.35zM7 12A5 5 0 1 0 7 2a5 5 0 0 0 0 10z"/></svg></button>
                    </div>
            </form>

        





<ol class="-secondary js-secondary-topbar-links drop-icons-responsively user-logged-in">
        <li class="-item searchbar-trigger js-searchbar-trigger"><a href="#" class="-link"><svg aria-hidden="true" class="svg-icon iconSearch" width="18" height="18" viewBox="0 0 18 18"><path d="M12.86 11.32L18 16.5 16.5 18l-5.18-5.14v-.35a7 7 0 1 1 1.19-1.19h.35zM7 12A5 5 0 1 0 7 2a5 5 0 0 0 0 10z"/></svg></a></li>
                <li class="-item">

            </li>
        <li class="-item inbox-button-item">
        <a href="https://stackexchange.com/users/10334661?tab=inbox" class="-link js-inbox-button" title="Recent inbox messages">
            <svg aria-hidden="true" class="svg-icon iconInbox" width="20" height="18" viewBox="0 0 20 18"><path d="M15.19 1H4.63c-.85 0-1.6.54-1.85 1.35L0 10.79V15c0 1.1.9 2 2 2h16a2 2 0 0 0 2-2v-4.21l-2.87-8.44A2 2 0 0 0 15.19 1zm-.28 10l-2 2h-6l-2-2H1.96L4.4 3.68A1 1 0 0 1 5.35 3h9.12a1 1 0 0 1 .95.68L17.86 11h-2.95z"/></svg>
            <span class="indicator-badge js-unread-count _important" style="display: none;"></span>
        </a>
    </li>
    <li class="-item achievements-button-item">
        <a href="https://stackexchange.com/users/10334661?tab=reputation" class="-link js-achievements-button" title="Recent achievements: reputation, badges, and privileges earned" data-unread-class="_highlighted-positive">
            <svg aria-hidden="true" class="svg-icon iconAchievements" width="18" height="18" viewBox="0 0 18 18"><path d="M15 2V1H3v1H0v4c0 1.6 1.4 3 3 3v1c.4 1.5 3 2.6 5 3v2H5s-1 1.5-1 2h10c0-.4-1-2-1-2h-3v-2c2-.4 4.6-1.5 5-3V9c1.6-.2 3-1.4 3-3V2h-3zM3 7c-.5 0-1-.5-1-1V4h1v3zm8.4 2.5L9 8 6.6 9.4l1-2.7L5 5h3l1-2.7L10 5h2.8l-2.3 1.8 1 2.7h-.1zM16 6c0 .5-.5 1-1 1V4h1v2z"/></svg>
            <span class="indicator-badge js-unread-count _positive" style="display: none;"></span>
        </a>
    </li>

    <li class="-item help-button-item" data-remove-order=1><a href="#" class="-link js-help-button" title="Help Center and other resources">
        <svg aria-hidden="true" class="svg-icon iconHelp" width="18" height="18" viewBox="0 0 18 18"><path d="M9 1a8 8 0 1 0 0 16A8 8 0 0 0 9 1zm.81 12.13c-.02.71-.55 1.15-1.24 1.13-.66-.02-1.17-.49-1.15-1.2.02-.72.56-1.18 1.22-1.16.7.03 1.2.51 1.17 1.23zM11.77 8a5.8 5.8 0 0 1-1.02.91l-.53.37c-.26.2-.42.43-.5.69a4 4 0 0 0-.09.75c0 .05-.03.16-.18.16H7.88c-.16 0-.18-.1-.18-.15.03-.66.12-1.21.4-1.66a5.29 5.29 0 0 1 1.43-1.22c.16-.12.28-.25.38-.39a1.34 1.34 0 0 0 .02-1.71c-.24-.31-.51-.46-1.03-.46-.51 0-.8.26-1.02.6-.21.33-.18.73-.18 1.1H5.75c0-1.38.35-2.25 1.1-2.76.52-.35 1.17-.5 1.93-.5 1 0 1.79.18 2.49.71.64.5.98 1.18.98 2.12 0 .57-.2 1.05-.48 1.44z"/></svg>
    </a></li>

    <li class="-item site-switcher-item">
        <a href="https://stackexchange.com" class="-link js-site-switcher-button js-gps-track" data-gps-track="site_switcher.show" title="A list of all 175 Stack Exchange sites">
            <svg aria-hidden="true" class="svg-icon iconStackExchange" width="18" height="18" viewBox="0 0 18 18"><path d="M1 13c0 1.1.9 2 2 2h8v3l3-3h1a2 2 0 0 0 2-2v-2H1v2zM15 1H3a2 2 0 0 0-2 2v2h16V3a2 2 0 0 0-2-2zM1 6h16v4H1V6z"/></svg>
        </a>
    </li>


    <li class="-dialog-container js-topbar-dialog-corral">


    <div class="topbar-dialog siteSwitcher-dialog dno">
        <div class="header">
            <h3><a href="https://stackoverflow.com">current community</a>
            </h3>
        </div>
        <div class="modal-content current-site-container">
            <ul class="current-site">
                    <li class="grid">
                            <div class="fl1">
            <a href="https://stackoverflow.com"
       class="current-site-link site-link js-gps-track grid gs8 gsx"
       data-id="1"
       data-gps-track="site_switcher.click({ item_type:3 })">
        <div class="favicon favicon-stackoverflow site-icon grid--cell" title="Stack Overflow"></div>
        <span class="grid--cell fl1">
            Stack Overflow
        </span>
    </a>

    </div>
    <div class="related-links">
            <a href="https://stackoverflow.com/help" class="js-gps-track"     data-gps-track="site_switcher.click({ item_type:14 })"
>help</a>
                    <a href="https://chat.stackoverflow.com" class="js-gps-track"     data-gps-track="site_switcher.click({ item_type:6 })"
>chat</a>
                    <a href="https://stackoverflow.com/users/logout" class="js-gps-track"     data-gps-track="site_switcher.click({ item_type:8 })"
>log out</a>
    </div>

                    </li>
                    <li class="related-site grid">
                            <div class="L-shaped-icon-container">
        <span class="L-shaped-icon"></span>
    </div>

                            <a href="https://meta.stackoverflow.com"
       class="site-link js-gps-track grid gs8 gsx"
       data-id="552"
       data-gps-track="site.switch({ target_site:552, item_type:3 }),site_switcher.click({ item_type:4 })">
        <div class="favicon favicon-stackoverflowmeta site-icon grid--cell" title="Meta Stack Overflow"></div>
        <span class="grid--cell fl1">
            Meta Stack Overflow
        </span>
    </a>

                    </li>
            </ul>
        </div>

        <div class="header" id="your-communities-header">
            <h3>
                    <a href="https://stackexchange.com/users/10334661/?tab=accounts">your communities</a>
            </h3>

                <a href="#" id="edit-pinned-sites">edit</a>
                <a href="#" id="cancel-pinned-sites" style="display: none;">cancel</a>
        </div>
        <div class="modal-content" id="your-communities-section">

                <ul class="my-sites">
                        <li>
                                <a href="https://askubuntu.com"
       class="site-link js-gps-track grid gs8 gsx"
       data-id="89"
       data-gps-track="site.switch({ target_site:89, item_type:3 }),site_switcher.click({ item_type:1 })">
        <div class="favicon favicon-askubuntu site-icon grid--cell" title="Ask Ubuntu"></div>
        <span class="grid--cell fl1">
            Ask Ubuntu
        </span>
            <span class="rep-score grid--cell">166</span>
    </a>

                        </li>
                        <li>
                                <a href="https://android.stackexchange.com"
       class="site-link js-gps-track grid gs8 gsx"
       data-id="139"
       data-gps-track="site.switch({ target_site:139, item_type:3 }),site_switcher.click({ item_type:1 })">
        <div class="favicon favicon-android site-icon grid--cell" title="Android Enthusiasts"></div>
        <span class="grid--cell fl1">
            Android Enthusiasts
        </span>
            <span class="rep-score grid--cell">1</span>
    </a>

                        </li>
                        <li>
                                <a href="https://stackoverflow.com"
       class="site-link js-gps-track grid gs8 gsx"
       data-id="1"
       data-gps-track="site.switch({ target_site:1, item_type:3 }),site_switcher.click({ item_type:1 })">
        <div class="favicon favicon-stackoverflow site-icon grid--cell" title="Stack Overflow"></div>
        <span class="grid--cell fl1">
            Stack Overflow
        </span>
            <span class="rep-score grid--cell">1</span>
    </a>

                        </li>
                </ul>
                    <div class="pinned-site-editor-container" style="display: none;">
                        <input type="hidden" name="fkey" value="6224af63245ce8b6331223f29a5c6b1f99cb35948a273df4dc1a2c2ebdb72328">
                        <input id="js-site-search-txt"
                                type="text"
                                class="site-filter-input"
                                value=""
                                placeholder="Add a Stack Exchange community"/>
                        <input type="submit" id="pin-site-btn" value="Add" disabled="disabled"/>
                        <ul class="js-found-sites found-sites"></ul>
                        <ul class="pinned-site-list sortable" data-custom-list="False">
                        </ul>
                        <input type="submit" value="Save" id="save-pinned-sites-btn" disabled="disabled"/>
                        <a href="#" id="reset-pinned-sites">reset to default list</a>
                    </div>
        </div>

        <div class="header">
            <h3><a href="https://stackexchange.com/sites">more stack exchange communities</a>
            </h3>
            <a href="https://stackoverflow.blog" class="fr">company blog</a>
        </div>
        <div class="modal-content">
                <div class="child-content"></div>
        </div>        
    </div>
            <div class="topbar-dialog help-dialog js-help-dialog dno">
        <div class="modal-content">
            <ul>
                                    <li>
                        <a href="/tour" class="js-gps-track" data-gps-track="help_popup.click({ item_type:1 })">
                            Tour
                            <span class="item-summary">
                                Start here for a quick overview of the site
                            </span>
                        </a>
                    </li>
                <li>
                    <a href="/help" class="js-gps-track" data-gps-track="help_popup.click({ item_type:4 })">
                        Help Center
                        <span class="item-summary">
                            Detailed answers to any questions you might have
                        </span>
                    </a>
                </li>
                        <li>
                            <a href="https://meta.stackoverflow.com" class="js-gps-track" data-gps-track="help_popup.click({ item_type:2 })">
                                Meta
                                <span class="item-summary">
                                    Discuss the workings and policies of this site
                                </span>
                            </a>
                        </li>
                        <li>
                            <a href="https://stackoverflow.com/company/about" class="js-gps-track" data-gps-track="help_popup.click({ item_type:6 })">
                                About Us
                                <span class="item-summary">
                                    Learn more about Stack Overflow the company
                                </span>
                            </a>
                        </li>
                        <li>
                            <a href="https://www.stackoverflowbusiness.com/?ref=topbar_help" class="js-gps-track" data-gps-track="help_popup.click({ item_type:7 })">
                                Business
                                <span class="item-summary">
                                    Learn more about hiring developers or posting ads with us
                                </span>
                            </a>
                        </li>
            </ul>
        </div>
    </div>
                    <div class="topbar-dialog leftnav-dialog js-leftnav-dialog dno">
                <div class="left-sidebar js-unpinned-left-sidebar" data-can-be="left-sidebar" data-is-here-when="sm"></div>
            </div>
    </li>
</ol>

    </div>
</header>

    <script>
        StackExchange.ready(function () { StackExchange.topbar.init({ observeSize: true }); });
        StackExchange.scrollPadding.setPaddingTop(50, 10);     </script>






    <div class="container">





<div id="left-sidebar" data-is-here-when="md lg" class="left-sidebar js-pinned-left-sidebar">
    <div class="left-sidebar--sticky-container js-sticky-leftnav">
            <nav role="navigation">
                <ol class="nav-links">
                    <li>    <li>
        <a
            href="/"
            class="pl8 js-gps-track nav-links--link"
            
            data-gps-track="top_nav.click({is_current:false, location:2, destination:8})">
Home        </a>
    </li>
</li>
                    <li>
                        <ol class="nav-links">
                                <li class="fs-fine tt-uppercase ml8 mt16 mb4 fc-light">Public</li>
                                <li class=" youarehere">
        <a id="nav-questions"
            href="/questions"
            class="pl8 js-gps-track nav-links--link -link__with-icon"
            
            data-gps-track="top_nav.click({is_current:true, location:2, destination:1})">
<svg aria-hidden="true" class="svg-icon iconGlobe" width="18" height="18" viewBox="0 0 18 18"><path d="M9 1a8 8 0 1 0 0 16A8 8 0 0 0 9 1zM8 15.32a6.4 6.4 0 0 1-5.23-7.75L7 11.68v.8c0 .88.12 1.32 1 1.32v1.52zm5.72-2c-.2-.66-1-1.32-1.72-1.32h-1v-2c0-.44-.56-1-1-1H6V7h1c.44 0 1-.56 1-1V5h2c.88 0 1.4-.72 1.4-1.6v-.33a6.4 6.4 0 0 1 2.32 10.24z"/></svg>                <span class="-link--channel-name">Stack Overflow</span>
        </a>
    </li>

    <li>
        <a id="nav-tags"
            href="/tags"
            class=" js-gps-track nav-links--link"
            
            data-gps-track="top_nav.click({is_current:false, location:2, destination:2})">
Tags        </a>
    </li>
    <li>
        <a id="nav-users"
            href="/users"
            class=" js-gps-track nav-links--link"
            
            data-gps-track="top_nav.click({is_current:false, location:2, destination:3})">
Users        </a>
    </li>
    <li>
        <a id="nav-jobs"
            href="/jobs?med=site-ui&amp;ref=jobs-tab"
            class=" js-gps-track nav-links--link"
            
            data-gps-track="top_nav.click({is_current:false, location:2, destination:6})">
Jobs        </a>
    </li>
                                                    </ol>
                    </li>
                        <li>
                            <ol class="nav-links">
                                        <li class="p6">
                                            <div class="ba bc-black-2 bar-sm p16 grid fd-column ps-relative overflow-hidden">
                                                <strong class="grid--cell">Teams</strong>
                                                <span class="grid--cell mt2 fs-caption fc-medium">Q&amp;A for work </span>
                                                <a href="https://stackoverflow.com/teams"
                                                   class="js-gps-track s-btn s-btn__outlined ta-center grid--cell mt12"
                                                   data-gps-track="teams.create.left-sidenav.click({ CampaignType: None})"
                                                   data-ga="[&quot;teams traffic&quot;,&quot;left nav cta&quot;,&quot;stackoverflow.com/teams&quot;,null,{&quot;dimension4&quot;:&quot;teams&quot;}]">
                                                    Learn More
                                                </a>

                                                <div class="ps-absolute t4 rn6">
                                                    <svg width="53" height="49" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M49 11l.2 31H18.9L9 49v-7H4V8h31" fill="#CCEAFF" /><path d="M44.5 19v-.3l-.2-.1-18-13-.1-.1H.5v33h4V46l.8-.6 9.9-6.9h29.3V19z" stroke="#1060E1" stroke-miterlimit="10" /><path d="M31 2l6-1.5 7 2V38H14.9L5 45v-7H1V6h25l5-4z" fill="#fff" /><path d="M7 16.5h13m-13 6h14m-14 6h18" stroke="#1060E1" stroke-miterlimit="10" /><path d="M39 30a14 14 0 1 0 0-28 14 14 0 0 0 0 28z" fill="#FFB935" /><path d="M50.5 14a13.5 13.5 0 1 1-27 0 13.5 13.5 0 0 1 27 0z" stroke="#F48024" stroke-miterlimit="10" /><path d="M32.5 21.5v-8h9v8h-9zm2-9.5V9.3A2.5 2.5 0 0 1 37 6.8a2.5 2.5 0 0 1 2.5 2.5V12h-5zm2 3v2m1-2v2" stroke="#fff" stroke-miterlimit="10" /></svg>
                                                </div>
                                            </div>
                                        </li>
                            </ol>
                        </li>
                </ol>
            </nav>
    </div>
</div>
   

        <div id="content" class="snippet-hidden">


            


<div itemprop="mainEntity" itemscope itemtype="http://schema.org/Question">
    <link itemprop="image" href="https://cdn.sstatic.net/Sites/stackoverflow/img/apple-touch-icon.png?v=c78bd457575a">

    <div class="inner-content clearfix">
    
    

            <div id="question-header" class="grid">
                    <h1 itemprop="name" class="grid--cell fs-headline1 fl1 ow-break-word"><a href="/questions/9/how-do-i-calculate-someones-age-in-c" class="question-hyperlink">{{ .Title }}</a></h1>

            
<div class="pl8 aside-cta grid--cell" role="navigation" aria-label="ask new question">
        <a href="/questions/ask" class="d-inline-flex ai-center ws-nowrap s-btn s-btn__primary" >
            Ask Question
        </a>
</div>

        </div>
            <div id="mainbar" role="main" aria-label="question and answers">

            

<div class="question" data-questionid="9"  id="question">

        <div class="post-layout">
            <div class="votecell post-layout--left">
                


            </div>

            
<div class="postcell post-layout--right">
    
    <div class="post-text" itemprop="text">

        {{.Body}}
    </div>

        <div class="post-taglist grid gs4 gsy fd-column">
            <div class="grid ps-relative d-block">
                <a href="/questions/tagged/c%23" class="post-tag" title="show questions tagged &#39;c#&#39;" rel="tag">c#</a> <a href="/questions/tagged/.net" class="post-tag" title="show questions tagged &#39;.net&#39;" rel="tag">.net</a> <a href="/questions/tagged/datetime" class="post-tag" title="show questions tagged &#39;datetime&#39;" rel="tag">datetime</a> 
            </div>
        </div>

    <div class="mb0 ">
        <div class="mt16 pt4 grid gs8 gsy fw-wrap jc-end ai-start">
            <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/q/9/7623252" title="short permalink to this question" class="short-link" itemprop="url" id="link-post-9">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Post is locked.">edit</a></div>
            </div>

                <div class="post-signature grid--cell">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/9/revisions" title="show all edits to this post">edited <span title="2018-04-21 17:48:14Z" class="relativetime">Apr 21 '18 at 17:48</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>                </div>
            <div class="post-signature grid--cell">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-9" href="/posts/9/revisions" title="show revision history for this post">
        29 revs, 25 users 37%<br><a href="/users/2455399">Shaik Raffi</a>
        </a>
    </div>
</div>

            </div>
        </div>
    </div>
    
</div>
                        <div class="special-status post-layout--full">
    <div class="question-status">
        <h2>                    <b>locked</b> by <a href="/users/3956566/yvette-colomb">Yvette Colomb</a><span class="mod-flair" title="moderator">&#9830;</span> <span dir="ltr"><span title="2018-04-21 17:49:09Z" class="relativetime">Apr 21 '18 at 17:49</span></span>
</h2>
        <p>This post has been locked while disputes about its content are being resolved. For more info <a href="https://meta.stackoverflow.com">visit meta</a>.</p><p>Read more about locked posts <a href="/help/locked-posts">here</a>.</p>
    </div>



            </div>
    
    <div class="post-layout--right">
	    <div id="comments-9" class="comments js-comments-container " data-post-id="9">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="4"
                    data-canpost="false"
                    data-cansee="false"
                    data-comments-unavailable="true"
                    data-addlink-disabled="true">


    <li id="comment-7043687" class="comment js-comment " data-comment-id="7043687">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="supernova">127</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">what all of the answers so far have missed is that it depends where the person was born and where they are right now.</span>
                
                    &ndash;&nbsp;<a href="/users/184025/yaur"
                       title="6,513 reputation"
                       class="comment-user">Yaur</a>
                <span class="comment-date" dir="ltr"><span title="2011-05-21 07:34:10Z" class="relativetime-clean">May 21 '11 at 7:34</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-9270342" class="comment js-comment " data-comment-id="9270342">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="supernova">37</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@Yaur: Just convert the time of now + birth into GMT/UTC, age is only a relative value, hence timezones are irrelevant. For determining the user&#39;s current timezone, you can use GeoLocating.</span>
                
                    &ndash;&nbsp;<a href="/users/155077/stefan-steiger"
                       title="46,148 reputation"
                       class="comment-user">Stefan Steiger</a>
                <span class="comment-date" dir="ltr"><span title="2011-10-03 10:20:32Z" class="relativetime-clean">Oct 3 '11 at 10:20</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-50688884" class="comment js-comment " data-comment-id="50688884">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">4</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">If we&#39;re taking into consideration @Yaur &#39;s suggestion of cross-timezone calculations, should Day Light Saving Time affect the calculation in any manner?</span>
                
                    &ndash;&nbsp;<a href="/users/2458438/ddm"
                       title="711 reputation"
                       class="comment-user">DDM</a>
                <span class="comment-date" dir="ltr"><span title="2015-07-11 03:42:39Z" class="relativetime-clean">Jul 11 '15 at 3:42</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-9" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid answering questions in comments."
                   >comments disabled on deleted / locked posts / reviews</a><span class="js-link-separator ">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>4</b> more comments</a>
        </div>         
    </div>        </div>
</div>


<div id="dfp-isb" class="everyonelovesstackoverflow everyoneloves__inline-sidebar"></div>
            <div id="answers">

                <a name="tab-top"></a>
                <div id="answers-header">
                    <div class="subheader answers-subheader">
                        <h2 data-answercount="63">
                                63 Answers
                            <span style="display:none;" itemprop="answerCount">63</span>
                        </h2>
                        <div>
                            

                        </div>
                    </div>
                </div>

  
<a name="1404"></a>
<div id="answer-1404" class="answer accepted-answer" data-answerid="1404"  itemprop="acceptedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="1404">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="1898">1898</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>An easy to understand and simple solution.</p>

<pre><code>// Save today's date.
var today = DateTime.Today;
// Calculate the age.
var age = today.Year - birthdate.Year;
// Go back to the year the person was born in case of a leap year
if (birthdate &gt; today.AddYears(-age)) age--;
</code></pre>

<p>However, this assumes you are looking for the <em>western</em> idea of age and not using <a href="https://en.wikipedia.org/wiki/East_Asian_age_reckoning" rel="noreferrer"><em>East Asian reckoning</em></a>.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-08-04T16:50:06"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/1404/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-1404">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/1404/revisions" title="show all edits to this post">edited <span title="2018-07-05 03:51:43Z" class="relativetime">Jul 5 '18 at 3:51</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-1404" href="/posts/1404/revisions" title="show revision history for this post">
        24 revs, 14 users 20%<br><a href="/users/212">Mike Polen</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-1404" class="comments js-comments-container " data-post-id="1404">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="30"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-286421" class="comment js-comment " data-comment-id="286421">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="supernova">227</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Just wanted to comment on DateTime.Now performance. If you don&#39;t need an accurate time zone value, use DateTime.UtcNow it&#39;s much faster.</span>
                
                    &ndash;&nbsp;<a href="/users/16032/jag"
                       title="1,061 reputation"
                       class="comment-user">JAG</a>
                <span class="comment-date" dir="ltr"><span title="2009-01-22 10:29:38Z" class="relativetime-clean">Jan 22 '09 at 10:29</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-1000180" class="comment js-comment " data-comment-id="1000180">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="supernova">92</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Given we&#39;re talking birthdays you can just use DateTime.Today given the time part has no relevance.</span>
                
                    &ndash;&nbsp;<a href="/users/29553/tristan-warner-smith"
                       title="7,457 reputation"
                       class="comment-user">Tristan Warner-Smith</a>
                <span class="comment-date" dir="ltr"><span title="2009-07-24 18:04:27Z" class="relativetime-clean">Jul 24 '09 at 18:04</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-1580306" class="comment js-comment " data-comment-id="1580306">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="supernova">74</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">This answer does not work with all locales and all ages. Several countries have skipped dates after the birth of current living people, including Russia (1918), Greece (1924) and Turkey (1926).</span>
                
                    &ndash;&nbsp;<a href="/users/1507543/lars-d"
                       title="4,949 reputation"
                       class="comment-user">Lars D</a>
                <span class="comment-date" dir="ltr"><span title="2009-11-09 22:09:27Z" class="relativetime-clean">Nov 9 '09 at 22:09</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-4535395" class="comment js-comment " data-comment-id="4535395">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="hot">27</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Actually, it&#39;s still not entirely correct. This code presumes that &#39;bday&#39; is the date-portion of a DateTime. It&#39;s an edge-case (I guess most people will just be passing dates and not date-times), but if you pass in a birthday as a date-and-time where the time is greater than 00:00:00 then you&#39;ll run into the bug Danvil pointed out. Setting bday = bday.Date fixes this.</span>
                
                    &ndash;&nbsp;<a href="/users/425038/%c3%98yvind"
                       title="1,031 reputation"
                       class="comment-user">&#216;yvind</a>
                <span class="comment-date" dir="ltr"><span title="2010-11-16 15:37:18Z" class="relativetime-clean">Nov 16 '10 at 15:37</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-7957266" class="comment js-comment " data-comment-id="7957266">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="supernova">101</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">The last line made me think too much. Instead how about: if (bday.AddYears(age) &gt; now) age--; This seems to be a more intuitive expression.</span>
                
                    &ndash;&nbsp;<a href="/users/184528/cdiggins"
                       title="11,994 reputation"
                       class="comment-user">cdiggins</a>
                <span class="comment-date" dir="ltr"><span title="2011-07-16 17:53:57Z" class="relativetime-clean">Jul 16 '11 at 17:53</span></span>
                        <span title="this comment was edited 1 time">
                            <svg aria-hidden="true" class="svg-icon va-text-bottom o50 iconPencilSm" width="14" height="14" viewBox="0 0 14 14"><path d="M8.37 3.7L2 10.11V12h1.88l6.37-6.43zM12.23 2.83L11.1 1.71a.5.5 0 0 0-.7 0L9.2 2.86l1.88 1.84 1.14-1.16a.5.5 0 0 0 0-.71z"/></svg>
                        </span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-1404" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>30</b> more comments</a>
        </div>         
    </div>    </div>
</div>
<div id="clc-mlb" class="everyonelovesstackoverflow everyoneloves__mid-leaderboard"></div>
  
<a name="11942"></a>
<div id="answer-11942" class="answer" data-answerid="11942"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="11942">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="948">948</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>This is a strange way to do it, but if you format the date to <code>yyyymmdd</code> and subtract the date of birth from the current date then drop the last 4 digits you've got the age :)</p>

<p>I don't know C#, but I believe this will work in any language.</p>

<pre><code>20080814 - 19800703 = 280111 
</code></pre>

<p>Drop the last 4 digits = <code>28</code>.</p>

<p>C# Code:</p>

<pre><code>int now = int.Parse(DateTime.Now.ToString("yyyyMMdd"));
int dob = int.Parse(dateOfBirth.ToString("yyyyMMdd"));
int age = (now - dob) / 10000;
</code></pre>

<p>Or alternatively without all the type conversion in the form of an extension method. Error checking omitted:</p>

<pre><code>public static Int32 GetAge(this DateTime dateOfBirth)
{
    var today = DateTime.Today;

    var a = (today.Year * 100 + today.Month) * 100 + today.Day;
    var b = (dateOfBirth.Year * 100 + dateOfBirth.Month) * 100 + dateOfBirth.Day;

    return (a - b) / 10000;
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-08-15T03:47:29"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/11942/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-11942">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/11942/revisions" title="show all edits to this post">edited <span title="2017-01-01 01:58:36Z" class="relativetime">Jan 1 '17 at 1:58</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-11942" href="/posts/11942/revisions" title="show revision history for this post">
        12 revs, 8 users 38%<br><a href="/users/1310">ScArcher2</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-11942" class="comments js-comments-container " data-post-id="11942">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="9"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-42574507" class="comment js-comment " data-comment-id="42574507">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="supernova">50</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Horray for <code>yyyymmdd</code>. The 1st database product I used on a <b>P</b>ersonal <b>C</b>omputer (circa 1981) stored dates in this format. Date manipulation of any kind was so much easier.</span>
                
                    &ndash;&nbsp;<a href="/users/463206/radarbob"
                       title="3,970 reputation"
                       class="comment-user">radarbob</a>
                <span class="comment-date" dir="ltr"><span title="2014-11-19 19:43:01Z" class="relativetime-clean">Nov 19 '14 at 19:43</span></span>
                        <span title="this comment was edited 1 time">
                            <svg aria-hidden="true" class="svg-icon va-text-bottom o50 iconPencilSm" width="14" height="14" viewBox="0 0 14 14"><path d="M8.37 3.7L2 10.11V12h1.88l6.37-6.43zM12.23 2.83L11.1 1.71a.5.5 0 0 0-.7 0L9.2 2.86l1.88 1.84 1.14-1.16a.5.5 0 0 0 0-.71z"/></svg>
                        </span>
                                                            </div>
        </div>
    </li>
    <li id="comment-50414522" class="comment js-comment " data-comment-id="50414522">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">2</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Actually this is great for usage on MS-SQL with datetime-fields (total days since 01-011900)</span>
                
                    &ndash;&nbsp;<a href="/users/1389444/patrik-eckebrecht"
                       title="1,193 reputation"
                       class="comment-user">Patrik Eckebrecht</a>
                <span class="comment-date" dir="ltr"><span title="2015-07-03 12:01:00Z" class="relativetime-clean">Jul 3 '15 at 12:01</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-52640216" class="comment js-comment " data-comment-id="52640216">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">2</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@numerek Please post your suggested modifications as their own answer. For what it&#39;s worth, the current year times 10000 is nowhere near an integer overflow, by two orders of magnitude.  20,150,000 vs 2,147,483,648</span>
                
                    &ndash;&nbsp;<a href="/users/29638/galacticcowboy"
                       title="10,525 reputation"
                       class="comment-user">GalacticCowboy</a>
                <span class="comment-date" dir="ltr"><span title="2015-09-03 20:23:57Z" class="relativetime-clean">Sep 3 '15 at 20:23</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-88733542" class="comment js-comment " data-comment-id="88733542">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">2</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@LongChalk <code>20180101 - 20171231 = 8870</code>. Drop the last 4 digits and you have (an implied) <code>0</code> for the age. How did you get <code>1</code>?</span>
                
                    &ndash;&nbsp;<a href="/users/2052655/rufus-l"
                       title="19,401 reputation"
                       class="comment-user">Rufus L</a>
                <span class="comment-date" dir="ltr"><span title="2018-06-14 20:36:33Z" class="relativetime-clean">Jun 14 '18 at 20:36</span></span>
                        <span title="this comment was edited 1 time">
                            <svg aria-hidden="true" class="svg-icon va-text-bottom o50 iconPencilSm" width="14" height="14" viewBox="0 0 14 14"><path d="M8.37 3.7L2 10.11V12h1.88l6.37-6.43zM12.23 2.83L11.1 1.71a.5.5 0 0 0-.7 0L9.2 2.86l1.88 1.84 1.14-1.16a.5.5 0 0 0 0-.71z"/></svg>
                        </span>
                                                            </div>
        </div>
    </li>
    <li id="comment-89306826" class="comment js-comment " data-comment-id="89306826">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">1</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@flindeberg Yeah, that&#39;s what I was saying to LongChalk...</span>
                
                    &ndash;&nbsp;<a href="/users/2052655/rufus-l"
                       title="19,401 reputation"
                       class="comment-user">Rufus L</a>
                <span class="comment-date" dir="ltr"><span title="2018-07-03 19:32:28Z" class="relativetime-clean">Jul 3 '18 at 19:32</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-11942" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>9</b> more comments</a>
        </div>         
    </div>    </div>
</div>

  
<a name="1595311"></a>
<div id="answer-1595311" class="answer" data-answerid="1595311"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="1595311">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="361">361</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I don't know how the wrong solution can be accepted.
The correct C# snippet was written by Michael Stum</p>

<p>Here is a test snippet:</p>

<pre><code>DateTime bDay = new DateTime(2000, 2, 29);
DateTime now = new DateTime(2009, 2, 28);
MessageBox.Show(string.Format("Test {0} {1} {2}",
                CalculateAgeWrong1(bDay, now),     // outputs 9
                CalculateAgeWrong2(bDay, now),     // outputs 9
                CalculateAgeCorrect(bDay, now)));  // outputs 8
</code></pre>

<p>Here you have the methods:</p>

<pre><code>public int CalculateAgeWrong1(DateTime birthDate, DateTime now)
{
    return new DateTime(now.Subtract(birthDate).Ticks).Year - 1;
}

public int CalculateAgeWrong2(DateTime birthDate, DateTime now)
{
    int age = now.Year - birthDate.Year;

    if (now &lt; birthDate.AddYears(age))
        age--;

    return age;
}

public int CalculateAgeCorrect(DateTime birthDate, DateTime now)
{
    int age = now.Year - birthDate.Year;

    if (now.Month &lt; birthDate.Month || (now.Month == birthDate.Month &amp;&amp; now.Day &lt; birthDate.Day))
        age--;

    return age;
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2009-10-20T15:07:22"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/1595311/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-1595311">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/1595311/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:04:29Z" class="relativetime">Feb 7 '16 at 0:04</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-1595311" href="/posts/1595311/revisions" title="show revision history for this post">
        4 revs, 4 users 78%<br><a href="/users/193184">RMA</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-1595311" class="comments js-comments-container " data-post-id="1595311">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="9"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-10251291" class="comment js-comment " data-comment-id="10251291">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">8</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Output was -Test 9 9 8</span>
                
                    &ndash;&nbsp;<a href="/users/220329/moxplod"
                       title="2,297 reputation"
                       class="comment-user">MoXplod</a>
                <span class="comment-date" dir="ltr"><span title="2011-11-29 20:52:52Z" class="relativetime-clean">Nov 29 '11 at 20:52</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-39517562" class="comment js-comment " data-comment-id="39517562">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="hot">28</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">While this code works, it asserts that a person born on a leap day attains the next year of age on March 1st on non-leap years, rather than on February 28th.  In reality, <i>either option may be correct</i>.  <a href="http://en.wikipedia.org/wiki/Leap_day#Births" rel="nofollow noreferrer">Wikipedia has something to say about this</a>.  So while your code is not &quot;wrong&quot;, neither is the accepted solution.</span>
                
                    &ndash;&nbsp;<a href="/users/634824/matt-johnson"
                       title="142,510 reputation"
                       class="comment-user">Matt Johnson</a>
                <span class="comment-date" dir="ltr"><span title="2014-08-17 05:44:53Z" class="relativetime-clean">Aug 17 '14 at 5:44</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-64084691" class="comment js-comment " data-comment-id="64084691">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">13</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@MattJohnson I think that&#39;s actually correct. If my bday was Feb 29, then Feb 28 my bday hasn&#39;t passed, and I should still be the same age as on Feb 27. On March 1, however, we have passed my bday and I should be the next age.  In the US, a business that sells alcohol will have a sign that says something like &quot;If you were born after this day in YYYY, you can&#39;t purchase alcohol&quot; (where YYYY changes every year). That means that someone born on Feb 29 cannot buy alcohol on Feb 28 in the year they turn 21 (most places), and lends support to the idea that they are not a year older until March 1.</span>
                
                    &ndash;&nbsp;<a href="/users/666675/jfren484"
                       title="688 reputation"
                       class="comment-user">jfren484</a>
                <span class="comment-date" dir="ltr"><span title="2016-07-12 17:18:36Z" class="relativetime-clean">Jul 12 '16 at 17:18</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-64088801" class="comment js-comment " data-comment-id="64088801">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">2</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@jfren484 - read the Wikipedia article.  It varies considerably across jurisdictions.</span>
                
                    &ndash;&nbsp;<a href="/users/634824/matt-johnson"
                       title="142,510 reputation"
                       class="comment-user">Matt Johnson</a>
                <span class="comment-date" dir="ltr"><span title="2016-07-12 19:26:17Z" class="relativetime-clean">Jul 12 '16 at 19:26</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-72320584" class="comment js-comment " data-comment-id="72320584">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">5</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@jfren484 Your claim has absolutely nothing to do with philosophy; but everything to do with <b><i>your own personal feeling</i></b>. When a person born on 29 Feb &quot;ages&quot; is largely unimportant unless the age forms a &#39;legal age boundary&#39; (e.g. Can buy alcohol, vote, get pension, join army, get driving license). Consider US drinking age (21 years): For most people that&#39;s 7670 days. It&#39;s 7671 days if born before 29 Feb in leap year or from 1 Mar before leap year. If born on 29 Feb: 28 Feb is 7670 days and 1 Mar is 7671 days. <b><i>The choice is arbitrary</i></b> it can go either way.</span>
                
                    &ndash;&nbsp;<a href="/users/224704/disillusioned"
                       title="12,667 reputation"
                       class="comment-user">Disillusioned</a>
                <span class="comment-date" dir="ltr"><span title="2017-03-04 10:06:22Z" class="relativetime-clean">Mar 4 '17 at 10:06</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-1595311" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>9</b> more comments</a>
        </div>         
    </div>    </div>
</div>

  
<a name="13733"></a>
<div id="answer-13733" class="answer" data-answerid="13733"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="13733">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="126">126</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I don't think any of the answers so far provide for cultures that calculate age differently.  See, for example, <a href="https://en.wikipedia.org/wiki/East_Asian_age_reckoning" rel="noreferrer">East Asian Age Reckoning</a> versus that in the West.</p>

<p>Any <em>real</em> answer has to include localization.  The <a href="https://en.wikipedia.org/wiki/Strategy_pattern" rel="noreferrer">Strategy Pattern</a> would probably be in order in this example.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-08-17T17:14:48"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/13733/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-13733">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/13733/revisions" title="show all edits to this post">edited <span title="2018-07-05 03:52:31Z" class="relativetime">Jul 5 '18 at 3:52</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-13733" href="/posts/13733/revisions" title="show revision history for this post">
        2 revs, 2 users 86%<br><a href="/users/1190">James A. Rosen</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-13733" class="comments js-comments-container " data-post-id="13733">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="4"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-222454" class="comment js-comment " data-comment-id="222454">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="hot">24</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">From the wikipedia article that you provided: &quot;In China and Japan it is used for traditional fortune-telling or religion, and it is disappearing in daily life between peoples in the city.&quot;</span>
                
                    &ndash;&nbsp;<a href="/users/36866/some"
                       title="36,073 reputation"
                       class="comment-user">some</a>
                <span class="comment-date" dir="ltr"><span title="2008-12-28 09:15:57Z" class="relativetime-clean">Dec 28 '08 at 9:15</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-3198782" class="comment js-comment " data-comment-id="3198782">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">7</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">@some -- Koreans still use this system primarily.</span>
                
                    &ndash;&nbsp;<a href="/users/292731/justin-l"
                       title="10,077 reputation"
                       class="comment-user">Justin L.</a>
                <span class="comment-date" dir="ltr"><span title="2010-06-25 09:15:37Z" class="relativetime-clean">Jun 25 '10 at 9:15</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-3551625" class="comment js-comment " data-comment-id="3551625">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="hot">21</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Actually this concept can be pretty important - people don&#39;t like being told their personal information incorrectly. As an example, half of my family lives in Malaysia and half in the UK. Right now my age is considered two years higher when I&#39;m with one side of my family than with the other.</span>
                
                    &ndash;&nbsp;<a href="/users/280780/phil-gan"
                       title="2,390 reputation"
                       class="comment-user">Phil Gan</a>
                <span class="comment-date" dir="ltr"><span title="2010-08-05 08:16:14Z" class="relativetime-clean">Aug 5 '10 at 8:16</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-18202002" class="comment js-comment " data-comment-id="18202002">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">6</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Not only us this system used primarily in Korea, but as a tourist discussing ages with locals, locals will politely refer to yourself an each other by their birth year. I&#39;m not 25, I&#39;m 87. I like this approach better. more of an &#39;international birthdatetime format&#39;</span>
                
                    &ndash;&nbsp;<a href="/users/14966/dean-rather"
                       title="18,370 reputation"
                       class="comment-user">Dean Rather</a>
                <span class="comment-date" dir="ltr"><span title="2012-11-12 05:56:46Z" class="relativetime-clean">Nov 12 '12 at 5:56</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-50687396" class="comment js-comment " data-comment-id="50687396">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">2</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">In Vietnam, in daily life even when Western system is used, age is calculated merely by subtracting the years. Date is excluded</span>
                
                    &ndash;&nbsp;<a href="/users/995714/phuclv"
                       title="16,221 reputation"
                       class="comment-user">phuclv</a>
                <span class="comment-date" dir="ltr"><span title="2015-07-11 00:56:49Z" class="relativetime-clean">Jul 11 '15 at 0:56</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-13733" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>4</b> more comments</a>
        </div>         
    </div>    </div>
</div>

  
<a name="5054317"></a>
<div id="answer-5054317" class="answer" data-answerid="5054317"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="5054317">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="103">103</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>The simple answer to this is to apply <code>AddYears</code> as shown below because this is the only native method to add years to the 29th of Feb. of leap years and obtain the correct result of the 28th of Feb. for common years. </p>

<p>Some feel that 1th of Mar. is the birthday of leaplings but neither .Net nor any official rule supports this, nor does common logic explain why some born in February should have 75% of their birthdays in another month.</p>

<p>Further, an Age method lends itself to be added as an extension to <code>DateTime</code>. By this you can obtain the age in the simplest possible way:</p>

<ol>
<li>List item</li>
</ol>

<p><strong>int age = birthDate.Age();</strong></p>

<pre><code>public static class DateTimeExtensions
{
    /// &lt;summary&gt;
    /// Calculates the age in years of the current System.DateTime object today.
    /// &lt;/summary&gt;
    /// &lt;param name="birthDate"&gt;The date of birth&lt;/param&gt;
    /// &lt;returns&gt;Age in years today. 0 is returned for a future date of birth.&lt;/returns&gt;
    public static int Age(this DateTime birthDate)
    {
        return Age(birthDate, DateTime.Today);
    }

    /// &lt;summary&gt;
    /// Calculates the age in years of the current System.DateTime object on a later date.
    /// &lt;/summary&gt;
    /// &lt;param name="birthDate"&gt;The date of birth&lt;/param&gt;
    /// &lt;param name="laterDate"&gt;The date on which to calculate the age.&lt;/param&gt;
    /// &lt;returns&gt;Age in years on a later day. 0 is returned as minimum.&lt;/returns&gt;
    public static int Age(this DateTime birthDate, DateTime laterDate)
    {
        int age;
        age = laterDate.Year - birthDate.Year;

        if (age &gt; 0)
        {
            age -= Convert.ToInt32(laterDate.Date &lt; birthDate.Date.AddYears(age));
        }
        else
        {
            age = 0;
        }

        return age;
    }
}
</code></pre>

<p>Now, run this test:</p>

<pre><code>class Program
{
    static void Main(string[] args)
    {
        RunTest();
    }

    private static void RunTest()
    {
        DateTime birthDate = new DateTime(2000, 2, 28);
        DateTime laterDate = new DateTime(2011, 2, 27);
        string iso = "yyyy-MM-dd";

        for (int i = 0; i &lt; 3; i++)
        {
            for (int j = 0; j &lt; 3; j++)
            {
                Console.WriteLine("Birth date: " + birthDate.AddDays(i).ToString(iso) + "  Later date: " + laterDate.AddDays(j).ToString(iso) + "  Age: " + birthDate.AddDays(i).Age(laterDate.AddDays(j)).ToString());
            }
        }

        Console.ReadKey();
    }
}
</code></pre>

<p>The critical date example is this:</p>

<p><strong>Birth date: 2000-02-29  Later date: 2011-02-28  Age: 11</strong></p>

<p>Output:</p>

<pre><code>{
    Birth date: 2000-02-28  Later date: 2011-02-27  Age: 10
    Birth date: 2000-02-28  Later date: 2011-02-28  Age: 11
    Birth date: 2000-02-28  Later date: 2011-03-01  Age: 11
    Birth date: 2000-02-29  Later date: 2011-02-27  Age: 10
    Birth date: 2000-02-29  Later date: 2011-02-28  Age: 11
    Birth date: 2000-02-29  Later date: 2011-03-01  Age: 11
    Birth date: 2000-03-01  Later date: 2011-02-27  Age: 10
    Birth date: 2000-03-01  Later date: 2011-02-28  Age: 10
    Birth date: 2000-03-01  Later date: 2011-03-01  Age: 11
}
</code></pre>

<p>And for the later date 2012-02-28:</p>

<pre><code>{
    Birth date: 2000-02-28  Later date: 2012-02-28  Age: 12
    Birth date: 2000-02-28  Later date: 2012-02-29  Age: 12
    Birth date: 2000-02-28  Later date: 2012-03-01  Age: 12
    Birth date: 2000-02-29  Later date: 2012-02-28  Age: 11
    Birth date: 2000-02-29  Later date: 2012-02-29  Age: 12
    Birth date: 2000-02-29  Later date: 2012-03-01  Age: 12
    Birth date: 2000-03-01  Later date: 2012-02-28  Age: 11
    Birth date: 2000-03-01  Later date: 2012-02-29  Age: 11
    Birth date: 2000-03-01  Later date: 2012-03-01  Age: 12
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2011-02-19T23:56:58"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/5054317/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-5054317">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/5054317/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:05:21Z" class="relativetime">Feb 7 '16 at 0:05</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-5054317" href="/posts/5054317/revisions" title="show revision history for this post">
        6 revs, 6 users 86%<br><a href="/users/624612">camelCasus</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-5054317" class="comments js-comments-container " data-post-id="5054317">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="1"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-89975713" class="comment js-comment " data-comment-id="89975713">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">3</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">A comment regarding having the 29th Feb birthday on 1st March, technically, having it on the 28th is too early (1 day early in fact). On the 1st is one day too late. But since the birthday is between, using the 1st to calculate the age in non-leap years makes more sense to me, since that person is indeed that old on March 1st (and 2nd and 3rd) every year, but not on Feb 28th.</span>
                
                    &ndash;&nbsp;<a href="/users/4890963/cyberclaw"
                       title="335 reputation"
                       class="comment-user">CyberClaw</a>
                <span class="comment-date" dir="ltr"><span title="2018-07-24 16:31:02Z" class="relativetime-clean">Jul 24 '18 at 16:31</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-5054317" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>1</b> more comment</a>
        </div>         
    </div>    </div>
</div>

  
<a name="168703"></a>
<div id="answer-168703" class="answer" data-answerid="168703"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="168703">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="80">80</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>My suggestion</p>

<pre><code>int age = (int) ((DateTime.Now - bday).TotalDays/365.242199);
</code></pre>

<p>That seems to have the year changing on the right date. (I spot tested up to age 107)</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-10-03T20:19:31"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/168703/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-168703">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/168703/revisions" title="show all edits to this post">edited <span title="2013-08-01 13:20:56Z" class="relativetime">Aug 1 '13 at 13:20</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-168703" href="/posts/168703/revisions" title="show revision history for this post">
        2 revs, 2 users 89%<br><a href="/users/12725">James Curran</a>

        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-168703" class="comments js-comments-container " data-post-id="168703">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="5"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-274821" class="comment js-comment " data-comment-id="274821">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">4</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Where does 365.255 come from?  I don&#39;t think this will work in general.</span>
                
                    &ndash;&nbsp;<a href="/users/4234/dreeves"
                       title="13,481 reputation"
                       class="comment-user">dreeves</a>
                <span class="comment-date" dir="ltr"><span title="2009-01-18 03:15:12Z" class="relativetime-clean">Jan 18 '09 at 3:15</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-280580" class="comment js-comment " data-comment-id="280580">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">13</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">365 for the days in a year. +0.25 for leap years. +0.005 for other corrections</span>
                
                    &ndash;&nbsp;<a href="/users/12725/james-curran"
                       title="86,304 reputation"
                       class="comment-user">James Curran</a>
                <span class="comment-date" dir="ltr"><span title="2009-01-20 16:04:46Z" class="relativetime-clean">Jan 20 '09 at 16:04</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-1041272" class="comment js-comment " data-comment-id="1041272">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="hot">22</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">I don&#39;t think Harry Patch would have appreciated your spot-testing methodology:  <a href="http://www.latimes.com/news/obituaries/la-me-harry-patch26-2009jul26,0,7608030.story" rel="nofollow noreferrer">latimes.com/news/obituaries/&hellip;</a></span>
                
                    &ndash;&nbsp;<a href="/users/14606/musigenesis"
                       title="61,431 reputation"
                       class="comment-user">MusiGenesis</a>
                <span class="comment-date" dir="ltr"><span title="2009-08-01 16:03:00Z" class="relativetime-clean">Aug 1 '09 at 16:03</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-4115551" class="comment js-comment " data-comment-id="4115551">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">10</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">The average length of a year in the Gregorian Calendar is 365.2425 days.</span>
                
                    &ndash;&nbsp;<a href="/users/287586/dan04"
                       title="63,867 reputation"
                       class="comment-user">dan04</a>
                <span class="comment-date" dir="ltr"><span title="2010-10-06 02:01:06Z" class="relativetime-clean">Oct 6 '10 at 2:01</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-6145760" class="comment js-comment " data-comment-id="6145760">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">11</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">^^ Because sometimes it&#39;s important. In my testing this fails on the persons birthday, it reports them younger than they are.</span>
                
                    &ndash;&nbsp;<a href="/users/23300/chadt"
                       title="6,368 reputation"
                       class="comment-user">ChadT</a>
                <span class="comment-date" dir="ltr"><span title="2011-03-25 05:27:55Z" class="relativetime-clean">Mar 25 '11 at 5:27</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-168703" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>5</b> more comments</a>
        </div>         
    </div>    </div>
</div>

  
<a name="229"></a>
<div id="answer-229" class="answer" data-answerid="229"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="229">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="69">69</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>Another function, not by me but found on the web and refined it a bit:</p>

<pre><code>public static int GetAge(DateTime birthDate)
{
    DateTime n = DateTime.Now; // To avoid a race condition around midnight
    int age = n.Year - birthDate.Year;

    if (n.Month &lt; birthDate.Month || (n.Month == birthDate.Month &amp;&amp; n.Day &lt; birthDate.Day))
        age--;

    return age;
}
</code></pre>

<p>Just two things that come into my mind: What about people from countries that do not use the gregorian calendar? DateTime.Now is in the server-specific culture i think. I have absolutely 0 knowledge about actually working with Asian calendars and I do not know if there is an easy way to convert dates between calendars, but just in case you're wondering about those chinese guys from the year 4660 :-)</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-08-01T21:46:12"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/229/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-229">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/229/revisions" title="show all edits to this post">edited <span title="2017-07-25 17:11:03Z" class="relativetime">Jul 25 '17 at 17:11</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-229" href="/posts/229/revisions" title="show revision history for this post">
        5 revs, 3 users 93%<br><a href="/users/91">Michael Stum</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-229" class="comments js-comments-container  dno" data-post-id="229">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="1"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-229" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>1</b> more comment</a>
        </div>         
    </div>    </div>
</div>

  
<a name="5623077"></a>
<div id="answer-5623077" class="answer" data-answerid="5623077"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="5623077">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="47">47</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>2 Main problems to solve are:</p>

<p><strong>1. Calculate Exact age</strong> - in years, months, days, etc.</p>

<p><strong>2. Calculate Generally perceived age</strong> - people usually do not care how old they exactly are, they just care when their birthday in the current year is.</p>

<hr>

<p>Solution for <strong>1</strong> is obvious:</p>

<pre><code>DateTime birth = DateTime.Parse("1.1.2000");
DateTime today = DateTime.Today;     //we usually don't care about birth time
TimeSpan age = today - birth;        //.NET FCL should guarantee this as precise
double ageInDays = age.TotalDays;    //total number of days ... also precise
double daysInYear = 365.2425;        //statistical value for 400 years
double ageInYears = ageInDays / daysInYear;  //can be shifted ... not so precise
</code></pre>

<hr>

<p>Solution for <strong>2</strong> is the one which is not so precise in determing total age, but is perceived as precise by people. People also usually use it, when they calculate their age "manually":</p>

<pre><code>DateTime birth = DateTime.Parse("1.1.2000");
DateTime today = DateTime.Today;
int age = today.Year - birth.Year;    //people perceive their age in years

if (today.Month &lt; birth.Month ||
   ((today.Month == birth.Month) &amp;&amp; (today.Day &lt; birth.Day)))
{
  age--;  //birthday in current year not yet reached, we are 1 year younger ;)
          //+ no birthday for 29.2. guys ... sorry, just wrong date for birth
}
</code></pre>

<p>Notes to 2.:</p>

<ul>
<li>This is my preferred solution</li>
<li>We cannot use DateTime.DayOfYear or TimeSpans, as they shift number of days in leap years</li>
<li>I have put there little more lines for readability</li>
</ul>

<p>Just one more note ... I would create 2 static overloaded methods for it, one for universal usage, second for usage-friendliness:</p>

<pre><code>public static int GetAge(DateTime bithDay, DateTime today) 
{ 
  //chosen solution method body
}

public static int GetAge(DateTime birthDay) 
{ 
  return GetAge(birthDay, DateTime.Now);
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2011-04-11T14:47:15"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/5623077/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-5623077">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/5623077/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:06:14Z" class="relativetime">Feb 7 '16 at 0:06</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-5623077" href="/posts/5623077/revisions" title="show revision history for this post">
        5 revs, 2 users 96%<br><a href="/users/702199">Thetam</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-5623077" class="comments js-comments-container  dno" data-post-id="5623077">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-5623077" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="877516"></a>
<div id="answer-877516" class="answer" data-answerid="877516"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="877516">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="45">45</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I am late to the party, but here's a one-liner:</p>

<pre><code>int age = new DateTime(DateTime.Now.Subtract(birthday).Ticks).Year-1;
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2009-05-18T12:36:51"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/877516/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-877516">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/877516/revisions" title="show all edits to this post">edited <span title="2009-12-12 13:04:04Z" class="relativetime">Dec 12 '09 at 13:04</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-877516" href="/posts/877516/revisions" title="show revision history for this post">
        2 revs, 2 users 80%<br><a href="/users/88600">SillyMonkey</a>

        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-877516" class="comments js-comments-container " data-post-id="877516">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-5467471" class="comment js-comment " data-comment-id="5467471">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="hot">22</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">This is broken. Made testable:   public static int CalculateAge(DateTime dateOfBirth, DateTime dateToCalculateAge)         {             return new DateTime(dateToCalculateAge.Subtract(dateOfBirth).Ticks).Year - 1;         } ...Gives age 14 when I input 1990-06-01 and calculate the age on the day BEFORE his 14th birthday (1990-05-31).</span>
                
                    &ndash;&nbsp;<a href="/users/77884/kjensen"
                       title="4,913 reputation"
                       class="comment-user">Kjensen</a>
                <span class="comment-date" dir="ltr"><span title="2011-02-05 21:42:19Z" class="relativetime-clean">Feb 5 '11 at 21:42</span></span>
                        <span title="this comment was edited 3 times">
                            <svg aria-hidden="true" class="svg-icon va-text-bottom o50 iconPencilSm" width="14" height="14" viewBox="0 0 14 14"><path d="M8.37 3.7L2 10.11V12h1.88l6.37-6.43zM12.23 2.83L11.1 1.71a.5.5 0 0 0-.7 0L9.2 2.86l1.88 1.84 1.14-1.16a.5.5 0 0 0 0-.71z"/></svg>
                        </span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-877516" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="3261"></a>
<div id="answer-3261" class="answer" data-answerid="3261"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="3261">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="37">37</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>This is the version we use here. It works, and it's fairly simple. It's the same idea as Jeff's but I think it's a little clearer because it separates out the logic for subtracting one, so it's a little easier to understand.</p>

<pre><code>public static int GetAge(this DateTime dateOfBirth, DateTime dateAsAt)
{
    return dateAsAt.Year - dateOfBirth.Year - (dateOfBirth.DayOfYear &lt; dateAsAt.DayOfYear ? 0 : 1);
}
</code></pre>

<p>You could expand the ternary operator to make it even clearer, if you think that sort of thing is unclear.</p>

<p>Obviously this is done as an extension method on <code>DateTime</code>, but clearly you can grab that one line of code that does the work and put it anywhere. Here we have another overload of the Extension method that passes in <code>DateTime.Now</code>, just for completeness.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-08-06T10:23:30"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/3261/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-3261">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/3261/revisions" title="show all edits to this post">edited <span title="2010-05-23 10:19:08Z" class="relativetime">May 23 '10 at 10:19</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-3261" href="/posts/3261/revisions" title="show revision history for this post">
        2 revs, 2 users 86%<br><a href="/users/489">Ch00k</a>

        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-3261" class="comments js-comments-container " data-post-id="3261">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="1"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-216533" class="comment js-comment " data-comment-id="216533">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="warm">5</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">I think this can be off by one day when exactly one of dateOfBirth or dateAsAt falls in a leap year. Consider the age of a person born on March 1, 2003 on February 29, 2004.  To rectify this, you need to do a lexicographic comparison of (Month, DayOfMonth) pairs and use that for the conditional.</span>
                
                    &ndash;&nbsp;<a href="/users/11173/doug-mcclean"
                       title="9,699 reputation"
                       class="comment-user">Doug McClean</a>
                <span class="comment-date" dir="ltr"><span title="2008-12-23 15:36:19Z" class="relativetime-clean">Dec 23 '08 at 15:36</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-3261" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>1</b> more comment</a>
        </div>         
    </div>    </div>
</div>

  
<a name="22"></a>
<div id="answer-22" class="answer" data-answerid="22"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="22">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="32">32</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>The best way that I know of because of leap years and everything is:</p>

<pre><code>DateTime birthDate = new DateTime(2000,3,1);
int age = (int)Math.Floor((DateTime.Now - birthDate).TotalDays / 365.25D);
</code></pre>

<p>Hope this helps.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-08-01T12:07:19"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/22/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-22">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/22/revisions" title="show all edits to this post">edited <span title="2008-08-01 15:26:37Z" class="relativetime">Aug 1 '08 at 15:26</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-22" href="/posts/22/revisions" title="show revision history for this post">
        3 revs<br><a href="/users/17">Nick Berardi</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-22" class="comments js-comments-container  dno" data-post-id="22">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-22" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="2280982"></a>
<div id="answer-2280982" class="answer" data-answerid="2280982"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="2280982">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="30">30</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I use this:</p>

<pre><code>public static class DateTimeExtensions
{
    public static int Age(this DateTime birthDate)
    {
        return Age(birthDate, DateTime.Now);
    }

    public static int Age(this DateTime birthDate, DateTime offsetDate)
    {
        int result=0;
        result = offsetDate.Year - birthDate.Year;

        if (offsetDate.DayOfYear &lt; birthDate.DayOfYear)
        {
              result--;
        }

        return result;
    }
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2010-02-17T13:32:36"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/2280982/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-2280982">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/2280982/revisions" title="show all edits to this post">edited <span title="2015-06-03 11:10:46Z" class="relativetime">Jun 3 '15 at 11:10</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-2280982" href="/posts/2280982/revisions" title="show revision history for this post">
        4 revs, 4 users 78%<br><a href="/users/173109">Elmer</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-2280982" class="comments js-comments-container  dno" data-post-id="2280982">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-2280982" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="18924226"></a>
<div id="answer-18924226" class="answer" data-answerid="18924226"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="18924226">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="29">29</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>
<div class="bounty-award-container grid--cell"><span class="bounty-award d-block ta-center" title="This answer has been awarded bounties worth 50 reputation by don kaka">+50</span></div>
</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>This gives "more detail" to this question. Maybe this is what you're looking for</p>

<pre><code>DateTime birth = new DateTime(1974, 8, 29);
DateTime today = DateTime.Now;
TimeSpan span = today - birth;
DateTime age = DateTime.MinValue + span;

// Make adjustment due to MinValue equalling 1/1/1
int years = age.Year - 1;
int months = age.Month - 1;
int days = age.Day - 1;

// Print out not only how many years old they are but give months and days as well
Console.Write("{0} years, {1} months, {2} days", years, months, days);
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2013-09-20T19:13:56"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/18924226/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-18924226">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/18924226/revisions" title="show all edits to this post">edited <span title="2013-10-15 12:16:32Z" class="relativetime">Oct 15 '13 at 12:16</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Sep 20 &#39;13 at 19:13. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-18924226" href="/posts/18924226/revisions" title="show revision history for this post">
        2 revs, 2 users 70%<br><a href="/users/2778315">Jacqueline Loriault</a>

        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-18924226" class="comments js-comments-container " data-post-id="18924226">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="3"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-29009988" class="comment js-comment " data-comment-id="29009988">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">1</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">timespan itself automatically takes into account leap years between 2 dates so I&#39;m not sure what your getting on about. I have asked on microsoft forums and microsoft has confirmed it takes into account leap years between 2 dates.</span>
                
                    &ndash;&nbsp;<a href="/users/2778315/jacqueline-loriault"
                       title="495 reputation"
                       class="comment-user">Jacqueline Loriault</a>
                <span class="comment-date" dir="ltr"><span title="2013-10-23 19:29:18Z" class="relativetime-clean">Oct 23 '13 at 19:29</span></span>
                                                            </div>
        </div>
    </li>
    <li id="comment-29030663" class="comment js-comment " data-comment-id="29030663">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">1</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">Consider the following TWO senarios. 1st DateTime.Now is 1/1/2001 and a child is born on 1/1/2000. 2000 is a leap year and the result will be 1years, 0 months and 1 days. In the second senarion DateTime.Now is 1/1/2002 and the child is born on 1/1/2001. In this case the result will be 1 years, 0 months and 0 days. That will happen because you are adding the timespan on a non-leap year. If DateTime.MinValue was a leap year then the results would be 1 year at the first and 0 years 11 months and 30 days. (Try it in your code).</span>
                
                    &ndash;&nbsp;<a href="/users/1643676/athanasios-kataras"
                       title="1,682 reputation"
                       class="comment-user">Athanasios Kataras</a>
                <span class="comment-date" dir="ltr"><span title="2013-10-24 10:31:59Z" class="relativetime-clean">Oct 24 '13 at 10:31</span></span>
                        <span title="this comment was edited 1 time">
                            <svg aria-hidden="true" class="svg-icon va-text-bottom o50 iconPencilSm" width="14" height="14" viewBox="0 0 14 14"><path d="M8.37 3.7L2 10.11V12h1.88l6.37-6.43zM12.23 2.83L11.1 1.71a.5.5 0 0 0-.7 0L9.2 2.86l1.88 1.84 1.14-1.16a.5.5 0 0 0 0-.71z"/></svg>
                        </span>
                                                            </div>
        </div>
    </li>
    <li id="comment-46485281" class="comment js-comment " data-comment-id="46485281">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">3</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">You are quite right it&#39;s not. But IF it was that would be the result. Why does it matter? It doesn&#39;t. In either case leap or not then there are examples where this does not work. That was what I wanted to show. The DIFF is correct. Span takes into account leap years. But ADDING to a base date is not. Try the examples in code and you will see I&#39;m right.</span>
                
                    &ndash;&nbsp;<a href="/users/1643676/athanasios-kataras"
                       title="1,682 reputation"
                       class="comment-user">Athanasios Kataras</a>
                <span class="comment-date" dir="ltr"><span title="2015-03-18 18:20:34Z" class="relativetime-clean">Mar 18 '15 at 18:20</span></span>
                        <span title="this comment was edited 1 time">
                            <svg aria-hidden="true" class="svg-icon va-text-bottom o50 iconPencilSm" width="14" height="14" viewBox="0 0 14 14"><path d="M8.37 3.7L2 10.11V12h1.88l6.37-6.43zM12.23 2.83L11.1 1.71a.5.5 0 0 0-.7 0L9.2 2.86l1.88 1.84 1.14-1.16a.5.5 0 0 0 0-.71z"/></svg>
                        </span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-18924226" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>3</b> more comments</a>
        </div>         
    </div>    </div>
</div>

  
<a name="24242"></a>
<div id="answer-24242" class="answer" data-answerid="24242"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="24242">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="25">25</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I have created a SQL Server User Defined Function to calculate someone's age, given their birthdate. This is useful when you need it as part of a query:</p>

<pre><code>using System;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

public partial class UserDefinedFunctions
{
    [SqlFunction(DataAccess = DataAccessKind.Read)]
    public static SqlInt32 CalculateAge(string strBirthDate)
    {
        DateTime dtBirthDate = new DateTime();
        dtBirthDate = Convert.ToDateTime(strBirthDate);
        DateTime dtToday = DateTime.Now;

        // get the difference in years
        int years = dtToday.Year - dtBirthDate.Year;

        // subtract another year if we're before the
        // birth day in the current year
        if (dtToday.Month &lt; dtBirthDate.Month || (dtToday.Month == dtBirthDate.Month &amp;&amp; dtToday.Day &lt; dtBirthDate.Day))
            years=years-1;

        int intCustomerAge = years;
        return intCustomerAge;
    }
};
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2008-08-23T13:58:02"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/24242/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-24242">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/24242/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:07:00Z" class="relativetime">Feb 7 '16 at 0:07</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-24242" href="/posts/24242/revisions" title="show revision history for this post">
        3 revs, 3 users 95%<br><a href="/users/2601">user2601</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-24242" class="comments js-comments-container  dno" data-post-id="24242">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-24242" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="877247"></a>
<div id="answer-877247" class="answer" data-answerid="877247"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="877247">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="23">23</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I've spent some time working on this and came up with this to calculate someone's age in years, months and days. I've tested against the Feb 29th problem and leap years and it seems to work, I'd appreciate any feedback:</p>

<pre><code>public void LoopAge(DateTime myDOB, DateTime FutureDate)
{
    int years = 0;
    int months = 0;
    int days = 0;

    DateTime tmpMyDOB = new DateTime(myDOB.Year, myDOB.Month, 1);

    DateTime tmpFutureDate = new DateTime(FutureDate.Year, FutureDate.Month, 1);

    while (tmpMyDOB.AddYears(years).AddMonths(months) &lt; tmpFutureDate)
    {
        months++;

        if (months &gt; 12)
        {
            years++;
            months = months - 12;
        }
    }

    if (FutureDate.Day &gt;= myDOB.Day)
    {
        days = days + FutureDate.Day - myDOB.Day;
    }
    else
    {
        months--;

        if (months &lt; 0)
        {
            years--;
            months = months + 12;
        }

        days +=
            DateTime.DaysInMonth(
                FutureDate.AddMonths(-1).Year, FutureDate.AddMonths(-1).Month
            ) + FutureDate.Day - myDOB.Day;

    }

    //add an extra day if the dob is a leap day
    if (DateTime.IsLeapYear(myDOB.Year) &amp;&amp; myDOB.Month == 2 &amp;&amp; myDOB.Day == 29)
    {
        //but only if the future date is less than 1st March
        if (FutureDate &gt;= new DateTime(FutureDate.Year, 3, 1))
            days++;
    }

}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2009-05-18T11:24:38"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/877247/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-877247">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/877247/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:07:23Z" class="relativetime">Feb 7 '16 at 0:07</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-877247" href="/posts/877247/revisions" title="show revision history for this post">
        3 revs, 3 users 69%<br>Jon
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-877247" class="comments js-comments-container  dno" data-post-id="877247">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-877247" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="16142434"></a>
<div id="answer-16142434" class="answer" data-answerid="16142434"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="16142434">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="22">22</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>Here's yet another answer:</p>

<pre><code>public static int AgeInYears(DateTime birthday, DateTime today)
{
    return ((today.Year - birthday.Year) * 372 + (today.Month - birthday.Month) * 31 + (today.Day - birthday.Day)) / 372;
}
</code></pre>

<p>This has been extensively unit-tested. It does look a bit "magic". The number 372 is the number of days there would be in a year if every month had 31 days.</p>

<p>The explanation of why it works (<a href="https://social.msdn.microsoft.com/Forums/en-US/csharplanguage/thread/ba4a98af-aab3-4c59-bdee-611334e502f2" rel="noreferrer">lifted from here</a>) is:</p>

<blockquote>
  <p>Let's set <code>Yn = DateTime.Now.Year, Yb = birthday.Year, Mn = DateTime.Now.Month, Mb = birthday.Month, Dn = DateTime.Now.Day, Db = birthday.Day</code></p>
  
  <p><code>age = Yn - Yb + (31*(Mn - Mb) + (Dn - Db)) / 372</code></p>
  
  <p>We know that what we need is either <code>Yn-Yb</code> if the date has already been reached, <code>Yn-Yb-1</code> if it has not.</p>
  
  <p>a) If <code>Mn&lt;Mb</code>, we have <code>-341 &lt;= 31*(Mn-Mb) &lt;= -31 and -30 &lt;= Dn-Db &lt;= 30</code></p>
  
  <p><code>-371 &lt;= 31*(Mn - Mb) + (Dn - Db) &lt;= -1</code></p>
  
  <p>With integer division</p>
  
  <p><code>(31*(Mn - Mb) + (Dn - Db)) / 372 = -1</code></p>
  
  <p>b) If <code>Mn=Mb</code> and <code>Dn&lt;Db</code>, we have <code>31*(Mn - Mb) = 0 and -30 &lt;= Dn-Db &lt;= -1</code></p>
  
  <p>With integer division, again</p>
  
  <p><code>(31*(Mn - Mb) + (Dn - Db)) / 372 = -1</code></p>
  
  <p>c) If <code>Mn&gt;Mb</code>, we have <code>31 &lt;= 31*(Mn-Mb) &lt;= 341 and -30 &lt;= Dn-Db &lt;= 30</code></p>
  
  <p><code>1 &lt;= 31*(Mn - Mb) + (Dn - Db) &lt;= 371</code></p>
  
  <p>With integer division</p>
  
  <p><code>(31*(Mn - Mb) + (Dn - Db)) / 372 = 0</code></p>
  
  <p>d) If <code>Mn=Mb</code> and <code>Dn&gt;Db</code>, we have <code>31*(Mn - Mb) = 0 and 1 &lt;= Dn-Db &lt;= 3</code>0</p>
  
  <p>With integer division, again</p>
  
  <p><code>(31*(Mn - Mb) + (Dn - Db)) / 372 = 0</code> </p>
  
  <p>e) If <code>Mn=Mb</code> and <code>Dn=Db</code>, we have <code>31*(Mn - Mb) + Dn-Db = 0</code></p>
  
  <p>and therefore <code>(31*(Mn - Mb) + (Dn - Db)) / 372 = 0</code></p>
</blockquote>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2013-04-22T08:19:51"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/16142434/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-16142434">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/16142434/revisions" title="show all edits to this post">edited <span title="2018-07-05 03:54:02Z" class="relativetime">Jul 5 '18 at 3:54</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Apr 22 &#39;13 at 8:19. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-16142434" href="/posts/16142434/revisions" title="show revision history for this post">
        3 revs, 3 users 83%<br><a href="/users/106159">Matthew Watson</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-16142434" class="comments js-comments-container " data-post-id="16142434">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">


    <li id="comment-88171433" class="comment js-comment " data-comment-id="88171433">
        <div class="js-comment-actions comment-actions">
            <div class="comment-score js-comment-edit-hide">
                    <span title="number of &#39;useful comment&#39; votes received"
                            class="cool">1</span>
            </div>
                    </div>
        <div class="comment-text js-comment-text-and-form">
            <div class="comment-body js-comment-edit-hide">
                
                <span class="comment-copy">I stumbled into this long and annoying discussion, and your solution is a really nice and small approach. Thanks for keeping it simple</span>
                
                    &ndash;&nbsp;<a href="/users/630535/nabuchodonossor"
                       title="1,982 reputation"
                       class="comment-user">nabuchodonossor</a>
                <span class="comment-date" dir="ltr"><span title="2018-05-29 09:43:22Z" class="relativetime-clean">May 29 '18 at 9:43</span></span>
                                                            </div>
        </div>
    </li>
            </ul>
	    </div>

        <div id="comments-link-16142434" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="13645039"></a>
<div id="answer-13645039" class="answer" data-answerid="13645039"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="13645039">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="19">19</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>Do we need to consider people who is smaller than 1 year? as Chinese culture, we describe small babies' age as 2 months or 4 weeks. </p>

<p>Below is my implementation, it is not as simple as what I imagined, especially to deal with date like 2/28. </p>

<pre><code>public static string HowOld(DateTime birthday, DateTime now)
{
    if (now &lt; birthday)
        throw new ArgumentOutOfRangeException("birthday must be less than now.");

    TimeSpan diff = now - birthday;
    int diffDays = (int)diff.TotalDays;

    if (diffDays &gt; 7)//year, month and week
    {
        int age = now.Year - birthday.Year;

        if (birthday &gt; now.AddYears(-age))
            age--;

        if (age &gt; 0)
        {
            return age + (age &gt; 1 ? " years" : " year");
        }
        else
        {// month and week
            DateTime d = birthday;
            int diffMonth = 1;

            while (d.AddMonths(diffMonth) &lt;= now)
            {
                diffMonth++;
            }

            age = diffMonth-1;

            if (age == 1 &amp;&amp; d.Day &gt; now.Day)
                age--;

            if (age &gt; 0)
            {
                return age + (age &gt; 1 ? " months" : " month");
            }
            else
            {
                age = diffDays / 7;
                return age + (age &gt; 1 ? " weeks" : " week");
            }
        }
    }
    else if (diffDays &gt; 0)
    {
        int age = diffDays;
        return age + (age &gt; 1 ? " days" : " day");
    }
    else
    {
        int age = diffDays;
        return "just born";
    }
}
</code></pre>

<p>This implementation has passed below test cases.</p>

<pre><code>[TestMethod]
public void TestAge()
{
    string age = HowOld(new DateTime(2011, 1, 1), new DateTime(2012, 11, 30));
    Assert.AreEqual("1 year", age);

    age = HowOld(new DateTime(2011, 11, 30), new DateTime(2012, 11, 30));
    Assert.AreEqual("1 year", age);

    age = HowOld(new DateTime(2001, 1, 1), new DateTime(2012, 11, 30));
    Assert.AreEqual("11 years", age);

    age = HowOld(new DateTime(2012, 1, 1), new DateTime(2012, 11, 30));
    Assert.AreEqual("10 months", age);

    age = HowOld(new DateTime(2011, 12, 1), new DateTime(2012, 11, 30));
    Assert.AreEqual("11 months", age);

    age = HowOld(new DateTime(2012, 10, 1), new DateTime(2012, 11, 30));
    Assert.AreEqual("1 month", age);

    age = HowOld(new DateTime(2008, 2, 28), new DateTime(2009, 2, 28));
    Assert.AreEqual("1 year", age);

    age = HowOld(new DateTime(2008, 3, 28), new DateTime(2009, 2, 28));
    Assert.AreEqual("11 months", age);

    age = HowOld(new DateTime(2008, 3, 28), new DateTime(2009, 3, 28));
    Assert.AreEqual("1 year", age);

    age = HowOld(new DateTime(2009, 1, 28), new DateTime(2009, 2, 28));
    Assert.AreEqual("1 month", age);

    age = HowOld(new DateTime(2009, 2, 1), new DateTime(2009, 3, 1));
    Assert.AreEqual("1 month", age);

    // NOTE.
    // new DateTime(2008, 1, 31).AddMonths(1) == new DateTime(2009, 2, 28);
    // new DateTime(2008, 1, 28).AddMonths(1) == new DateTime(2009, 2, 28);
    age = HowOld(new DateTime(2009, 1, 31), new DateTime(2009, 2, 28));
    Assert.AreEqual("4 weeks", age);

    age = HowOld(new DateTime(2009, 2, 1), new DateTime(2009, 2, 28));
    Assert.AreEqual("3 weeks", age);

    age = HowOld(new DateTime(2009, 2, 1), new DateTime(2009, 3, 1));
    Assert.AreEqual("1 month", age);

    age = HowOld(new DateTime(2012, 11, 5), new DateTime(2012, 11, 30));
    Assert.AreEqual("3 weeks", age);

    age = HowOld(new DateTime(2012, 11, 1), new DateTime(2012, 11, 30));
    Assert.AreEqual("4 weeks", age);

    age = HowOld(new DateTime(2012, 11, 20), new DateTime(2012, 11, 30));
    Assert.AreEqual("1 week", age);

    age = HowOld(new DateTime(2012, 11, 25), new DateTime(2012, 11, 30));
    Assert.AreEqual("5 days", age);

    age = HowOld(new DateTime(2012, 11, 29), new DateTime(2012, 11, 30));
    Assert.AreEqual("1 day", age);

    age = HowOld(new DateTime(2012, 11, 30), new DateTime(2012, 11, 30));
    Assert.AreEqual("just born", age);

    age = HowOld(new DateTime(2000, 2, 29), new DateTime(2009, 2, 28));
    Assert.AreEqual("8 years", age);

    age = HowOld(new DateTime(2000, 2, 29), new DateTime(2009, 3, 1));
    Assert.AreEqual("9 years", age);

    Exception e = null;

    try
    {
        age = HowOld(new DateTime(2012, 12, 1), new DateTime(2012, 11, 30));
    }
    catch (ArgumentOutOfRangeException ex)
    {
        e = ex;
    }

    Assert.IsTrue(e != null);
}
</code></pre>

<p>Hope it's helpful.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2012-11-30T12:13:21"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/13645039/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-13645039">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/13645039/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:18:38Z" class="relativetime">Feb 7 '16 at 0:18</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Nov 30 &#39;12 at 12:13. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-13645039" href="/posts/13645039/revisions" title="show revision history for this post">
        2 revs, 2 users 73%<br><a href="/users/1254006">rockXrock</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-13645039" class="comments js-comments-container  dno" data-post-id="13645039">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-13645039" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="3513146"></a>
<div id="answer-3513146" class="answer" data-answerid="3513146"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="3513146">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="18">18</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>Keeping it simple (and possibly stupid:)).</p>

<pre><code>DateTime birth = new DateTime(1975, 09, 27, 01, 00, 00, 00);
TimeSpan ts = DateTime.Now - birth;
Console.WriteLine("You are approximately " + ts.TotalSeconds.ToString() + " seconds old.");
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2010-08-18T14:29:10"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/3513146/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-3513146">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        answered <a href="/posts/3513146/revisions" title="show all edits to this post"><span title="2010-08-18 14:29:10Z" class="relativetime">Aug 18 '10 at 14:29</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-3513146" href="/posts/3513146/revisions" title="show revision history for this post">
        user181261
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-3513146" class="comments js-comments-container  dno" data-post-id="3513146">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="1"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-3513146" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>1</b> more comment</a>
        </div>         
    </div>    </div>
</div>

  
<a name="18898663"></a>
<div id="answer-18898663" class="answer" data-answerid="18898663"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="18898663">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="17">17</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<pre><code>TimeSpan diff = DateTime.Now - birthdayDateTime;
string age = String.Format("{0:%y} years, {0:%M} months, {0:%d}, days old", diff);
</code></pre>

<p>I'm not sure how exactly you'd like it returned to you, so I just made a readable string.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2013-09-19T15:18:53"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/18898663/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-18898663">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        answered <a href="/posts/18898663/revisions" title="show all edits to this post"><span title="2013-09-19 15:18:53Z" class="relativetime">Sep 19 '13 at 15:18</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Sep 19 &#39;13 at 15:18. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-18898663" href="/posts/18898663/revisions" title="show revision history for this post">
        Dakotah Hicock
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-18898663" class="comments js-comments-container  dno" data-post-id="18898663">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-18898663" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="3869003"></a>
<div id="answer-3869003" class="answer" data-answerid="3869003"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="3869003">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="17">17</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>The simplest way I've ever found is this. It works correctly for the US and western europe locales. Can't speak to other locales, especially places like China. 4 extra compares, at most, following the initial computation of age.</p>

<pre><code>public int AgeInYears(DateTime birthDate, DateTime referenceDate)
{
  Debug.Assert(referenceDate &gt;= birthDate, 
               "birth date must be on or prior to the reference date");

  DateTime birth = birthDate.Date;
  DateTime reference = referenceDate.Date;
  int years = (reference.Year - birth.Year);

  //
  // an offset of -1 is applied if the birth date has 
  // not yet occurred in the current year.
  //
  if (reference.Month &gt; birth.Month);
  else if (reference.Month &lt; birth.Month) 
    --years;
  else // in birth month
  {
    if (reference.Day &lt; birth.Day)
      --years;
  }

  return years ;
}
</code></pre>

<p>I was looking over the answers to this and noticed that nobody has made reference to regulatory/legal implications of leap day births. For instance, <a href="https://en.wikipedia.org/wiki/February_29#Births" rel="nofollow noreferrer">per Wikipedia</a>, if you're born on February 29th in various jurisdictions, you're non-leap year birthday varies:</p>

<ul>
<li>In the United Kingdom and Hong Kong: it's the ordinal day of the year, so the next day, March 1st is your birthday.</li>
<li>In New Zealand: it's the previous day, February 28th for the purposes of driver licencing, and March 1st for other purposes.</li>
<li>Taiwan: it's February 28th.</li>
</ul>

<p>And as near as I can tell, in the US, the statutes are silent on the matter, leaving it up to the common law and to how various regulatory bodies define things in their regulations.</p>

<p>To that end, an improvement:</p>

<pre><code>public enum LeapDayRule
{
  OrdinalDay     = 1 ,
  LastDayOfMonth = 2 ,
}

static int ComputeAgeInYears(DateTime birth, DateTime reference, LeapYearBirthdayRule ruleInEffect)
{
  bool isLeapYearBirthday = CultureInfo.CurrentCulture.Calendar.IsLeapDay(birth.Year, birth.Month, birth.Day);
  DateTime cutoff;

  if (isLeapYearBirthday &amp;&amp; !DateTime.IsLeapYear(reference.Year))
  {
    switch (ruleInEffect)
    {
      case LeapDayRule.OrdinalDay:
        cutoff = new DateTime(reference.Year, 1, 1)
                             .AddDays(birth.DayOfYear - 1);
        break;

      case LeapDayRule.LastDayOfMonth:
        cutoff = new DateTime(reference.Year, birth.Month, 1)
                             .AddMonths(1)
                             .AddDays(-1);
        break;

      default:
        throw new InvalidOperationException();
    }
  }
  else
  {
    cutoff = new DateTime(reference.Year, birth.Month, birth.Day);
  }

  int age = (reference.Year - birth.Year) + (reference &gt;= cutoff ? 0 : -1);
  return age &lt; 0 ? 0 : age;
}
</code></pre>

<p>It should be noted that this code assumes:</p>

<ul>
<li>A western (European) reckoning of age, and</li>
<li>A calendar, like the Gregorian calendar that inserts a single leap day at the end of a month.</li>
</ul>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2010-10-06T01:49:50"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/3869003/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-3869003">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/3869003/revisions" title="show all edits to this post">edited <span title="2018-07-05 03:53:35Z" class="relativetime">Jul 5 '18 at 3:53</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-3869003" href="/posts/3869003/revisions" title="show revision history for this post">
        8 revs, 5 users 66%<br><a href="/users/467473">Nicholas Carey</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-3869003" class="comments js-comments-container  dno" data-post-id="3869003">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-3869003" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="13531544"></a>
<div id="answer-13531544" class="answer" data-answerid="13531544"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="13531544">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="16">16</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>This is not a direct answer, but more of a philosophical reasoning about the problem at hand from a quasi-scientific point of view.</p>

<p>I would argue that the question does not specify the unit nor culture in which to measure age, most answers seem to assume an integer annual representation. The SI-unit for time is <code>second</code>, ergo the correct generic answer should be (of course assuming normalized <code>DateTime</code> and taking no regard whatsoever to relativistic effects):</p>

<pre><code>var lifeInSeconds = (DateTime.Now.Ticks - then.Ticks)/TickFactor;
</code></pre>

<p>In the Christian way of calculating age in years:</p>

<pre><code>var then = ... // Then, in this case the birthday
var now = DateTime.UtcNow;
int age = now.Year - then.Year;
if (now.AddYears(-age) &lt; then) age--;
</code></pre>

<p>In finance there is a similar problem when calculating something often referred to as the <em>Day Count Fraction</em>, which roughly is a number of years for a given period. And the age issue is really a time measuring issue.</p>

<p>Example for the actual/actual (counting all days "correctly") convention:</p>

<pre><code>DateTime start, end = .... // Whatever, assume start is before end

double startYearContribution = 1 - (double) start.DayOfYear / (double) (DateTime.IsLeapYear(start.Year) ? 366 : 365);
double endYearContribution = (double)end.DayOfYear / (double)(DateTime.IsLeapYear(end.Year) ? 366 : 365);
double middleContribution = (double) (end.Year - start.Year - 1);

double DCF = startYearContribution + endYearContribution + middleContribution;
</code></pre>

<p>Another quite common way to measure time generally is by "serializing" (the dude who named this date convention must seriously have been trippin'):</p>

<pre><code>DateTime start, end = .... // Whatever, assume start is before end
int days = (end - start).Days;
</code></pre>

<p>I wonder how long we have to go before a relativistic age in seconds becomes more useful than the rough approximation of earth-around-sun-cycles during one's lifetime so far :) Or in other words, when a period must be given a location or a function representing motion for itself to be valid :)</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2012-11-23T15:00:31"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/13531544/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-13531544">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/13531544/revisions" title="show all edits to this post">edited <span title="2017-10-31 11:32:39Z" class="relativetime">Oct 31 '17 at 11:32</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Nov 23 &#39;12 at 15:00. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-13531544" href="/posts/13531544/revisions" title="show revision history for this post">
        2 revs, 2 users 97%<br><a href="/users/691294">flindeberg</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-13531544" class="comments js-comments-container  dno" data-post-id="13531544">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-13531544" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="1011981"></a>
<div id="answer-1011981" class="answer" data-answerid="1011981"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="1011981">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="16">16</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>Here is a solution.</p>

<pre><code>DateTime dateOfBirth = new DateTime(2000, 4, 18);
DateTime currentDate = DateTime.Now;

int ageInYears = 0;
int ageInMonths = 0;
int ageInDays = 0;

ageInDays = currentDate.Day - dateOfBirth.Day;
ageInMonths = currentDate.Month - dateOfBirth.Month;
ageInYears = currentDate.Year - dateOfBirth.Year;

if (ageInDays &lt; 0)
{
    ageInDays += DateTime.DaysInMonth(currentDate.Year, currentDate.Month);
    ageInMonths = ageInMonths--;

    if (ageInMonths &lt; 0)
    {
        ageInMonths += 12;
        ageInYears--;
    }
}

if (ageInMonths &lt; 0)
{
    ageInMonths += 12;
    ageInYears--;
}

Console.WriteLine("{0}, {1}, {2}", ageInYears, ageInMonths, ageInDays);
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2009-06-18T10:35:21"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/1011981/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-1011981">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/1011981/revisions" title="show all edits to this post">edited <span title="2018-02-22 16:36:45Z" class="relativetime">Feb 22 '18 at 16:36</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-1011981" href="/posts/1011981/revisions" title="show revision history for this post">
        2 revs, 2 users 71%<br><a href="/users/21995">Rajeshwaran S P</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-1011981" class="comments js-comments-container  dno" data-post-id="1011981">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="1"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-1011981" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link  dno"
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   ></a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link " title="expand to show all comments on this post" href=# onclick="">show <b>1</b> more comment</a>
        </div>         
    </div>    </div>
</div>

  
<a name="26529035"></a>
<div id="answer-26529035" class="answer" data-answerid="26529035"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="26529035">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="15">15</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>This is one of the most accurate answer that is able to resolve the birthday of 29th of Feb compare to any year of 28th Feb.</p>

<pre><code>public int GetAge(DateTime birthDate)
{
    int age = DateTime.Now.Year - birthDate.Year;

    if (birthDate.DayOfYear &gt; DateTime.Now.DayOfYear)
        age--;

    return age;
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2014-10-23T13:18:40"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/26529035/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-26529035">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/26529035/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:07:42Z" class="relativetime">Feb 7 '16 at 0:07</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Oct 23 &#39;14 at 13:18. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-26529035" href="/posts/26529035/revisions" title="show revision history for this post">
        2 revs, 2 users 89%<br><a href="/users/520848">mjb</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-26529035" class="comments js-comments-container  dno" data-post-id="26529035">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-26529035" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="21276626"></a>
<div id="answer-21276626" class="answer" data-answerid="21276626"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="21276626">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="14">14</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I have a customized method to calculate age, plus a bonus validation message just in case it helps:</p>

<pre><code>public void GetAge(DateTime dob, DateTime now, out int years, out int months, out int days)
{
    years = 0;
    months = 0;
    days = 0;

    DateTime tmpdob = new DateTime(dob.Year, dob.Month, 1);
    DateTime tmpnow = new DateTime(now.Year, now.Month, 1);

    while (tmpdob.AddYears(years).AddMonths(months) &lt; tmpnow)
    {
        months++;
        if (months &gt; 12)
        {
            years++;
            months = months - 12;
        }
    }

    if (now.Day &gt;= dob.Day)
        days = days + now.Day - dob.Day;
    else
    {
        months--;
        if (months &lt; 0)
        {
            years--;
            months = months + 12;
        }
        days += DateTime.DaysInMonth(now.AddMonths(-1).Year, now.AddMonths(-1).Month) + now.Day - dob.Day;
    }

    if (DateTime.IsLeapYear(dob.Year) &amp;&amp; dob.Month == 2 &amp;&amp; dob.Day == 29 &amp;&amp; now &gt;= new DateTime(now.Year, 3, 1))
        days++;

}   

private string ValidateDate(DateTime dob) //This method will validate the date
{
    int Years = 0; int Months = 0; int Days = 0;

    GetAge(dob, DateTime.Now, out Years, out Months, out Days);

    if (Years &lt; 18)
        message =  Years + " is too young. Please try again on your 18th birthday.";
    else if (Years &gt;= 65)
        message = Years + " is too old. Date of Birth must not be 65 or older.";
    else
        return null; //Denotes validation passed
}
</code></pre>

<p>Method call here and pass out datetime value (MM/dd/yyyy if server set to USA locale). Replace this with anything a messagebox or any container to display:</p>

<pre><code>DateTime dob = DateTime.Parse("03/10/1982");  

string message = ValidateDate(dob);

lbldatemessage.Visible = !StringIsNullOrWhitespace(message);
lbldatemessage.Text = message ?? ""; //Ternary if message is null then default to empty string
</code></pre>

<p>Remember you can format the message any way you like.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2014-01-22T07:23:00"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/21276626/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-21276626">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/21276626/revisions" title="show all edits to this post">edited <span title="2018-01-02 09:45:44Z" class="relativetime">Jan 2 '18 at 9:45</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Jan 22 &#39;14 at 7:23. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-21276626" href="/posts/21276626/revisions" title="show revision history for this post">
        7 revs, 3 users 49%<br><a href="/users/1271898">Knickerless-Noggins</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-21276626" class="comments js-comments-container  dno" data-post-id="21276626">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-21276626" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="5229568"></a>
<div id="answer-5229568" class="answer" data-answerid="5229568"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="5229568">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="13">13</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>How about this solution? </p>

<pre><code>static string CalcAge(DateTime birthDay)
{
    DateTime currentDate = DateTime.Now;         
    int approximateAge = currentDate.Year - birthDay.Year;
    int daysToNextBirthDay = (birthDay.Month * 30 + birthDay.Day) - 
        (currentDate.Month * 30 + currentDate.Day) ;

    if (approximateAge == 0 || approximateAge == 1)
    {                
        int month =  Math.Abs(daysToNextBirthDay / 30);
        int days = Math.Abs(daysToNextBirthDay % 30);

        if (month == 0)
            return "Your age is: " + daysToNextBirthDay + " days";

        return "Your age is: " + month + " months and " + days + " days"; ;
    }

    if (daysToNextBirthDay &gt; 0)
        return "Your age is: " + --approximateAge + " Years";

    return "Your age is: " + approximateAge + " Years"; ;
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2011-03-08T07:25:02"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/5229568/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-5229568">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/5229568/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:14:20Z" class="relativetime">Feb 7 '16 at 0:14</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-5229568" href="/posts/5229568/revisions" title="show revision history for this post">
        4 revs, 4 users 66%<br><a href="/users/649407">Doron</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-5229568" class="comments js-comments-container  dno" data-post-id="5229568">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-5229568" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="3652116"></a>
<div id="answer-3652116" class="answer" data-answerid="3652116"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="3652116">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="11">11</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<pre><code>private int GetAge(int _year, int _month, int _day
{
    DateTime yourBirthDate= new DateTime(_year, _month, _day);

    DateTime todaysDateTime = DateTime.Today;
    int noOfYears = todaysDateTime.Year - yourBirthDate.Year;

    if (DateTime.Now.Month &lt; yourBirthDate.Month ||
        (DateTime.Now.Month == yourBirthDate.Month &amp;&amp; DateTime.Now.Day &lt; yourBirthDate.Day))
    {
        noOfYears--;
    }

    return  noOfYears;
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2010-09-06T14:09:01"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/3652116/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-3652116">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/3652116/revisions" title="show all edits to this post">edited <span title="2016-02-07 00:13:55Z" class="relativetime">Feb 7 '16 at 0:13</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-3652116" href="/posts/3652116/revisions" title="show revision history for this post">
        3 revs, 3 users 67%<br><a href="/users/440670">AEMLoviji</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-3652116" class="comments js-comments-container  dno" data-post-id="3652116">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-3652116" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="5989087"></a>
<div id="answer-5989087" class="answer" data-answerid="5989087"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="5989087">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="9">9</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>The following approach (extract from <a href="https://www.codeproject.com/Articles/168662/Time-Period-Library-for-NET" rel="nofollow noreferrer">Time Period Library for .NET</a> class <em>DateDiff</em>) considers the calendar of the culture info:</p>

<pre><code>// ----------------------------------------------------------------------
private static int YearDiff( DateTime date1, DateTime date2 )
{
  return YearDiff( date1, date2, DateTimeFormatInfo.CurrentInfo.Calendar );
} // YearDiff

// ----------------------------------------------------------------------
private static int YearDiff( DateTime date1, DateTime date2, Calendar calendar )
{
  if ( date1.Equals( date2 ) )
  {
    return 0;
  }

  int year1 = calendar.GetYear( date1 );
  int month1 = calendar.GetMonth( date1 );
  int year2 = calendar.GetYear( date2 );
  int month2 = calendar.GetMonth( date2 );

  // find the the day to compare
  int compareDay = date2.Day;
  int compareDaysPerMonth = calendar.GetDaysInMonth( year1, month1 );
  if ( compareDay &gt; compareDaysPerMonth )
  {
    compareDay = compareDaysPerMonth;
  }

  // build the compare date
  DateTime compareDate = new DateTime( year1, month2, compareDay,
    date2.Hour, date2.Minute, date2.Second, date2.Millisecond );
  if ( date2 &gt; date1 )
  {
    if ( compareDate &lt; date1 )
    {
      compareDate = compareDate.AddYears( 1 );
    }
  }
  else
  {
    if ( compareDate &gt; date1 )
    {
      compareDate = compareDate.AddYears( -1 );
    }
  }
  return year2 - calendar.GetYear( compareDate );
} // YearDiff
</code></pre>

<p>Usage:</p>

<pre><code>// ----------------------------------------------------------------------
public void CalculateAgeSamples()
{
  PrintAge( new DateTime( 2000, 02, 29 ), new DateTime( 2009, 02, 28 ) );
  // &gt; Birthdate=29.02.2000, Age at 28.02.2009 is 8 years
  PrintAge( new DateTime( 2000, 02, 29 ), new DateTime( 2012, 02, 28 ) );
  // &gt; Birthdate=29.02.2000, Age at 28.02.2012 is 11 years
} // CalculateAgeSamples

// ----------------------------------------------------------------------
public void PrintAge( DateTime birthDate, DateTime moment )
{
  Console.WriteLine( "Birthdate={0:d}, Age at {1:d} is {2} years", birthDate, moment, YearDiff( birthDate, moment ) );
} // PrintAge
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2011-05-13T08:12:55"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/5989087/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-5989087">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/5989087/revisions" title="show all edits to this post">edited <span title="2018-07-05 03:54:55Z" class="relativetime">Jul 5 '18 at 3:54</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-5989087" href="/posts/5989087/revisions" title="show revision history for this post">
        2 revs, 2 users 99%<br>user687474
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-5989087" class="comments js-comments-container  dno" data-post-id="5989087">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-5989087" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="7046204"></a>
<div id="answer-7046204" class="answer" data-answerid="7046204"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="7046204">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="8">8</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I used ScArcher2's solution for an accurate Year calculation of a persons age but I needed to take it further and calculate their Months and Days along with the Years.</p>

<pre><code>    public static Dictionary&lt;string,int&gt; CurrentAgeInYearsMonthsDays(DateTime? ndtBirthDate, DateTime? ndtReferralDate)
    {
        //----------------------------------------------------------------------
        // Can't determine age if we don't have a dates.
        //----------------------------------------------------------------------
        if (ndtBirthDate == null) return null;
        if (ndtReferralDate == null) return null;

        DateTime dtBirthDate = Convert.ToDateTime(ndtBirthDate);
        DateTime dtReferralDate = Convert.ToDateTime(ndtReferralDate);

        //----------------------------------------------------------------------
        // Create our Variables
        //----------------------------------------------------------------------
        Dictionary&lt;string, int&gt; dYMD = new Dictionary&lt;string,int&gt;();
        int iNowDate, iBirthDate, iYears, iMonths, iDays;
        string sDif = "";

        //----------------------------------------------------------------------
        // Store off current date/time and DOB into local variables
        //---------------------------------------------------------------------- 
        iNowDate = int.Parse(dtReferralDate.ToString("yyyyMMdd"));
        iBirthDate = int.Parse(dtBirthDate.ToString("yyyyMMdd"));

        //----------------------------------------------------------------------
        // Calculate Years
        //----------------------------------------------------------------------
        sDif = (iNowDate - iBirthDate).ToString();
        iYears = int.Parse(sDif.Substring(0, sDif.Length - 4));

        //----------------------------------------------------------------------
        // Store Years in Return Value
        //----------------------------------------------------------------------
        dYMD.Add("Years", iYears);

        //----------------------------------------------------------------------
        // Calculate Months
        //----------------------------------------------------------------------
        if (dtBirthDate.Month &gt; dtReferralDate.Month)
            iMonths = 12 - dtBirthDate.Month + dtReferralDate.Month - 1;
        else
            iMonths = dtBirthDate.Month - dtReferralDate.Month;

        //----------------------------------------------------------------------
        // Store Months in Return Value
        //----------------------------------------------------------------------
        dYMD.Add("Months", iMonths);

        //----------------------------------------------------------------------
        // Calculate Remaining Days
        //----------------------------------------------------------------------
        if (dtBirthDate.Day &gt; dtReferralDate.Day)
            //Logic: Figure out the days in month previous to the current month, or the admitted month.
            //       Subtract the birthday from the total days which will give us how many days the person has lived since their birthdate day the previous month.
            //       then take the referral date and simply add the number of days the person has lived this month.

            //If referral date is january, we need to go back to the following year's December to get the days in that month.
            if (dtReferralDate.Month == 1)
                iDays = DateTime.DaysInMonth(dtReferralDate.Year - 1, 12) - dtBirthDate.Day + dtReferralDate.Day;       
            else
                iDays = DateTime.DaysInMonth(dtReferralDate.Year, dtReferralDate.Month - 1) - dtBirthDate.Day + dtReferralDate.Day;       
        else
            iDays = dtReferralDate.Day - dtBirthDate.Day;             

        //----------------------------------------------------------------------
        // Store Days in Return Value
        //----------------------------------------------------------------------
        dYMD.Add("Days", iDays);

        return dYMD;
}
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2011-08-12T20:53:56"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/7046204/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-7046204">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/7046204/revisions" title="show all edits to this post">edited <span title="2012-10-14 12:05:16Z" class="relativetime">Oct 14 '12 at 12:05</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-7046204" href="/posts/7046204/revisions" title="show revision history for this post">
        3 revs, 2 users 99%<br><a href="/users/892460">Dylan Hayes</a>

        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-7046204" class="comments js-comments-container  dno" data-post-id="7046204">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-7046204" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="38121726"></a>
<div id="answer-38121726" class="answer" data-answerid="38121726"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="38121726">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="8">8</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>SQL version:</p>

<pre><code>declare @dd smalldatetime = '1980-04-01'
declare @age int = YEAR(GETDATE())-YEAR(@dd)
if (@dd&gt; DATEADD(YYYY, -@age, GETDATE())) set @age = @age -1

print @age  
</code></pre>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2016-06-30T11:24:56"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/38121726/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-38121726">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        answered <a href="/posts/38121726/revisions" title="show all edits to this post"><span title="2016-06-30 11:24:56Z" class="relativetime">Jun 30 '16 at 11:24</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Jun 30 &#39;16 at 11:24. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-38121726" href="/posts/38121726/revisions" title="show revision history for this post">
        xenedia
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-38121726" class="comments js-comments-container  dno" data-post-id="38121726">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-38121726" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>

  
<a name="6719204"></a>
<div id="answer-6719204" class="answer" data-answerid="6719204"  itemprop="suggestedAnswer" itemscope itemtype="http://schema.org/Answer">
    <div class="post-layout">
            <div class="votecell post-layout--left">
                

<div class="js-voting-container grid fd-column ai-stretch gs4 fc-black-200" data-post-id="6719204">
        <button class="js-vote-up-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is useful (click again to undo)" aria-pressed="false" aria-label="up vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowUpLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 26h32L18 10z"/></svg></button>
        <div class="js-vote-count grid--cell fc-black-500 fs-title grid fd-column ai-center" itemprop="upvoteCount" data-value="7">7</div>
        <button class="js-vote-down-btn grid--cell s-btn s-btn__unset c-pointer" title="This answer is not useful (click again to undo)" aria-pressed="false" aria-label="down vote" data-selected-classes="fc-theme-primary"><svg aria-hidden="true" class="svg-icon m0 iconArrowDownLg" width="36" height="36" viewBox="0 0 36 36"><path d="M2 10h32L18 26z"/></svg></button>


                <div class="js-accepted-answer-indicator grid--item fc-green-500 ta-center p4 d-none" title="loading when this answer was accepted..." tabindex="0" role="note" aria-label="accepted"><svg aria-hidden="true" class="svg-icon iconCheckmarkLg" width="36" height="36" viewBox="0 0 36 36"><path d="M6 14l8 8L30 6v8L14 30l-8-8z"/></svg></div>

</div>

            </div>
            

<div class="answercell post-layout--right">
    
    <div class="post-text" itemprop="text">
<p>I've made one small change to <a href="https://stackoverflow.com/questions/9/how-do-i-calculate-someones-age-in-c/1404#1404">Mark Soen's</a> answer: I've rewriten the third line so that the expression can be parsed a bit more easily.</p>

<pre><code>public int AgeInYears(DateTime bday)
{
    DateTime now = DateTime.Today;
    int age = now.Year - bday.Year;            
    if (bday.AddYears(age) &gt; now) 
        age--;
    return age;
}
</code></pre>

<p>I've also made it into a function for the sake of clarity.</p>
    </div>
    <div class="grid mb0 fw-wrap ai-start jc-end gs8 gsy">
        <time itemprop="dateCreated" datetime="2011-07-16T18:01:50"></time>
    <div class="grid--cell mr16" style="flex: 1 1 100px;">
<div class="post-menu"><a href="/a/6719204/7623252" title="short permalink to this answer" class="short-link" itemprop="url" id="link-post-6719204">share</a><span class="lsep">|</span><a href="#" class="js-error-click" title="revise and improve this post" data-error-message="Question is locked.">edit</a></div>                    </div>
    <div class="post-signature grid--cell fl0">
<div class="user-info ">
    <div class="user-action-time">
        <a href="/posts/6719204/revisions" title="show all edits to this post">edited <span title="2018-02-22 16:38:57Z" class="relativetime">Feb 22 '18 at 16:38</span></a>
    </div>
    <div class="user-gravatar32">
        
    </div>
    <div class="user-details">
        
        <div class="-flair">
            
        </div>
    </div>
</div>    </div>
            


    <div class="post-signature grid--cell fl0">
<div class="user-info">
    <div class="user-details">
        <span class="community-wiki" title="This post is community owned as of Aug 16 &#39;11 at 19:40. Votes do not generate reputation, and it can be edited by users with 100 rep">
            community wiki
        </span>
    </div><br>
    <div class="user-details">
        <a id="history-6719204" href="/posts/6719204/revisions" title="show revision history for this post">
        3 revs, 2 users 74%<br><a href="/users/184528">cdiggins</a>
        </a>
    </div>
</div>

    </div>
    </div>
    
</div>

    
    <div class="post-layout--right">
	    <div id="comments-6719204" class="comments js-comments-container  dno" data-post-id="6719204">
            <ul class="comments-list js-comments-list"
                    data-remaining-comments-count="0"
                    data-canpost="false"
                    data-cansee="true"
                    data-comments-unavailable="false"
                    data-addlink-disabled="true">

            </ul>
	    </div>

        <div id="comments-link-6719204" data-rep=50 data-reg=true>

                <a class="js-add-link comments-link disabled-link "
                   title="Use comments to ask for more information or suggest improvements. Avoid comments like “+1” or “thanks”."
                   >add a comment</a><span class="js-link-separator dno">&nbsp;|&nbsp;</span>
            <a class="js-show-link comments-link dno" title="expand to show all comments on this post" href=# onclick=""></a>
        </div>         
    </div>    </div>
</div>
<div class="pager-answers">
        




 <span class="page-numbers current">1</span>         <a href="/questions/9/how-do-i-calculate-someones-age-in-c?page=2&amp;tab=votes#tab-top" title="go to page 2"> <span class="page-numbers">2</span> </a> 
        <a href="/questions/9/how-do-i-calculate-someones-age-in-c?page=3&amp;tab=votes#tab-top" title="go to page 3"> <span class="page-numbers">3</span> </a> 
        <a href="/questions/9/how-do-i-calculate-someones-age-in-c?page=2&amp;tab=votes#tab-top" rel="next" title="go to page 2"> <span class="page-numbers next"> next</span> </a> 

    </div>                


                        <h2 class="bottom-notice" data-loc="1">
Not the answer you&#39;re looking for?                            Browse other questions tagged <a href="/questions/tagged/c%23" class="post-tag" title="show questions tagged &#39;c#&#39;" rel="tag">c#</a> <a href="/questions/tagged/.net" class="post-tag" title="show questions tagged &#39;.net&#39;" rel="tag">.net</a> <a href="/questions/tagged/datetime" class="post-tag" title="show questions tagged &#39;datetime&#39;" rel="tag">datetime</a>  or <a href="/questions/ask">ask your own question</a>.                        </h2>
            </div>
        </div>
                <div id="sidebar" class="show-votes" role="complementary" aria-label="sidebar">

<script>
    window.showNewUser = false;

</script>

<div class="module newuser newuser-greeting" id="newuser-box" style="display:none;">
    <h4>Hello World!</h4>
    <div>
        <p>This is a collaboratively edited question and answer site for <b>professional and enthusiast programmers</b>. It's 100% free.</p><p>Got a question about the site itself? <a href="https://meta.stackoverflow.com">meta</a> is the place to talk about things like what questions are appropriate, what tags we should use, etc.</p>
        <p class="ar">
                <a href="/tour?mnu=1">about &raquo;</a>&nbsp;&nbsp;&nbsp;
            <a href="/help?mnu=1">help &raquo;</a>
                &nbsp; <a href="https://meta.stackoverflow.com">meta &raquo;</a>
        </p>
    </div>
</div>
    <script>
    if (showNewUser) {
        document.getElementById('newuser-box').style.display = '';
    }
    </script>                            <div class="module question-stats">
                    <table id="qinfo">
                        <tr>
                            <td>
                                <p class="label-key">asked</p>
                            </td>
                            <td style="padding-left: 10px">
                                <p class="label-key" title="2008-07-31 23:40:59Z">
                                    <b>
                                        <time itemprop="dateCreated" datetime="2008-07-31T23:40:59">10 years, 8 months ago</time>
                                    </b>
                                </p>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <p class="label-key">viewed</p>
                            </td>

                            <td style="padding-left: 10px">
                                <p class="label-key">
                                    <b>528,736 times</b>
                                </p>
                            </td>
                        </tr>
                            <tr>
                                <td>
                                    <p class="label-key">active</p>
                                </td>
                                <td style="padding-left: 10px">
                                    <p class="label-key"><b><a href="?lastactivity" class="lastactivity-link" title="2019-04-08 22:58:20Z">5 days ago</a></b></p>
                                </td>
                            </tr>
                    </table>
                </div>
            <div class="module community-bulletin" data-tracker="cb=1">
    <div class="related">
                <div class="bulletin-title">
                    Blog
                </div>
                <hr />
    <div class="spacer">
        <div class="bulletin-item-type">
                <a href="https://stackoverflow.blog/2019/04/09/the-2019-stack-overflow-developer-survey-results-are-in/" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Blog&quot;,&quot;https://stackoverflow.blog/2019/04/09/the-2019-stack-overflow-developer-survey-results-are-in/&quot;,null,null]">
<div class="favicon favicon-stackexchangemeta" title="Meta Stack Exchange"></div>                </a>
        </div>
        <div class="bulletin-item-content">
            <a href="https://stackoverflow.blog/2019/04/09/the-2019-stack-overflow-developer-survey-results-are-in/" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Blog&quot;,&quot;https://stackoverflow.blog/2019/04/09/the-2019-stack-overflow-developer-survey-results-are-in/&quot;,null,null]">The 2019 Stack Overflow Developer Survey Results Are In</a>
        </div>
        <br class="cbt" />
    </div>
                <div class="bulletin-title">
                    Featured on Meta
                </div>
                <hr />
    <div class="spacer">
        <div class="bulletin-item-type">
                <a href="https://meta.stackexchange.com/questions/326611/announcing-the-arrival-of-valued-associate-679-cesar-manara" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackexchange.com/questions/326611/announcing-the-arrival-of-valued-associate-679-cesar-manara&quot;,null,null]">
<div class="favicon favicon-stackexchangemeta" title="Meta Stack Exchange"></div>                </a>
        </div>
        <div class="bulletin-item-content">
            <a href="https://meta.stackexchange.com/questions/326611/announcing-the-arrival-of-valued-associate-679-cesar-manara" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackexchange.com/questions/326611/announcing-the-arrival-of-valued-associate-679-cesar-manara&quot;,null,null]">Announcing the arrival of Valued Associate #679: Cesar Manara</a>
        </div>
        <br class="cbt" />
    </div>
    <div class="spacer">
        <div class="bulletin-item-type">
                <a href="https://meta.stackexchange.com/questions/326718/planned-maintenance-scheduled-april-17-18-2019-at-0000utc-800pm-us-eastern" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackexchange.com/questions/326718/planned-maintenance-scheduled-april-17-18-2019-at-0000utc-800pm-us-eastern&quot;,null,null]">
<div class="favicon favicon-stackexchangemeta" title="Meta Stack Exchange"></div>                </a>
        </div>
        <div class="bulletin-item-content">
            <a href="https://meta.stackexchange.com/questions/326718/planned-maintenance-scheduled-april-17-18-2019-at-0000utc-800pm-us-eastern" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackexchange.com/questions/326718/planned-maintenance-scheduled-april-17-18-2019-at-0000utc-800pm-us-eastern&quot;,null,null]">Planned maintenance scheduled April 17/18, 2019 at 00:00UTC (8:00pm US/Eastern)</a>
        </div>
        <br class="cbt" />
    </div>
    <div class="spacer">
        <div class="bulletin-item-type">
                <a href="https://meta.stackoverflow.com/questions/381671/the-ask-question-wizard-is-live" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackoverflow.com/questions/381671/the-ask-question-wizard-is-live&quot;,null,null]">
<div class="favicon favicon-stackoverflowmeta" title="Meta Stack Overflow"></div>                </a>
        </div>
        <div class="bulletin-item-content">
            <a href="https://meta.stackoverflow.com/questions/381671/the-ask-question-wizard-is-live" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackoverflow.com/questions/381671/the-ask-question-wizard-is-live&quot;,null,null]">The Ask Question Wizard is Live!</a>
        </div>
        <br class="cbt" />
    </div>
    <div class="spacer">
        <div class="bulletin-item-type">
                <a href="https://meta.stackoverflow.com/questions/382721/data-science-time-april-2019-and-salary-with-experience" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackoverflow.com/questions/382721/data-science-time-april-2019-and-salary-with-experience&quot;,null,null]">
<div class="favicon favicon-stackoverflowmeta" title="Meta Stack Overflow"></div>                </a>
        </div>
        <div class="bulletin-item-content">
            <a href="https://meta.stackoverflow.com/questions/382721/data-science-time-april-2019-and-salary-with-experience" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Featured on Meta&quot;,&quot;https://meta.stackoverflow.com/questions/382721/data-science-time-april-2019-and-salary-with-experience&quot;,null,null]">Data science time! April 2019 and salary with experience</a>
        </div>
        <br class="cbt" />
    </div>
                <div class="bulletin-title">
                    Hot Meta Posts
                </div>
                <hr />
    <div class="spacer">
        <div class="bulletin-item-type">
                <span title="Vote score (upvotes - downvotes)">36</span>
        </div>
        <div class="bulletin-item-content">
            <a href="https://meta.stackoverflow.com/questions/382780/can-the-stack-exchange-app-become-open-source" class="question-hyperlink" data-ga="[&quot;community bulletin board&quot;,&quot;Hot Meta Posts&quot;,&quot;https://meta.stackoverflow.com/questions/382780/can-the-stack-exchange-app-become-open-source&quot;,null,null]">Can the Stack Exchange app become open source?</a>
        </div>
        <br class="cbt" />
    </div>
    </div>
</div>
<div id="clc-tsb" class="everyonelovesstackoverflow everyoneloves__top-sidebar"></div>
            <div id="hireme"></div>
<script>;try{(function(a){function b(a){return'string'==typeof a?document.getElementById(a):a}function c(a){return a=b(a),!!a&&'none'===getComputedStyle(a).display}function d(a){return!c(a)}function e(a){return!!a}function f(a){return /^\s*$/.test(b(a).innerHTML)}function g(a){var b=a.style;b.height=b.maxHeight=b.minHeight='auto',b.display='none',[].forEach.call(a.children,g)}function h(a,b){var c;return function(){return a&&(c=a.call(b||this,arguments),a=null),c}}function i(a){var b=document.createElement('script');b.src=a,document.body.appendChild(b)}function j(a){return k([],a)}function k(a,b){return a.push=function(a){return b(),delete this.push,this.push(a)},a}function l(){try{return!new Function('return async()=>{};')}catch(a){return!0}}function m(){return'undefined'!=typeof googletag&&!!googletag.apiReady}function n(){m()||(googletag={cmd:j(A)})}function o(){var a=document.createElement('div');a.className='adsbox',a.id='clc-abd',a.style.position='absolute',a.style.pointerEvents='none',a.innerHTML='&nbsp;',document.body.appendChild(a)}function p(a){var b=a.serviceName,c=a.slot,d=a.lineItemId;try{var e=c.getSlotElementId(),f=[];e||f.push('id=0');var h=document.getElementById(e);if(!e||h?h.hasAttribute('data-clc-stalled')&&f.push('st=1'):f.push('el=0'),0!==f.length)return void B(f.join('&'));-1===x.dh.indexOf(d)?h.setAttribute('data-clc-prefilled','true'):g(h),h.setAttribute('data-clc-ready','true')}catch(a){var i=document.querySelector('#dfp-tsb, #dfp-isb, #clc-tsb');i&&i.setAttribute('data-clc-ready','true'),B('e=1')}}function q(){return Object.keys(F.ids)}function r(a){var b=F.ids[a],c=F.slots[b];'function'==typeof c&&(c=c(a));return{path:'/'+C+'/'+E+'/'+b+'/'+D,sizes:c,zone:b}}function s(a){return!(clc.collapse&&void 0!==clc.collapse[a])||!!clc.collapse[a]}function t(a,b){'dfp-isb'===a&&b.setTargeting('Sidebar',['Inline']),'dfp-tsb'===a&&b.setTargeting('Sidebar',['Right']);var c=r(a),d=c.path,e=c.sizes,f=c.zone,g=googletag.defineSlot(d,e,a);s(f)&&g.setCollapseEmptyDiv(!0,!0),g.addService(b),!1}function u(b){var c=a.dfp&&a.dfp.targeting||{};Object.keys(c).forEach(function(a){b.setTargeting(a,c[a])})}function v(a){var g=a.map(b).filter(e);return{eligible:g.filter(f).filter(d).map(function(a){return a.id}),ineligible:g.filter(c)}}function w(b){if(void 0===b&&(b=q()),!m())return n(),void googletag.cmd.push(function(){return w(b)});var c=v(b),d=c.eligible,e=c.ineligible;if(e.forEach(g),0!==d.length){o(),googletag.destroySlots();var f=googletag.pubads();f.enableSingleRequest(),a.sreEvent||(f.addEventListener('slotRenderEnded',p),a.sreEvent=!0),u(f),d.forEach(function(a){return t(a,f)}),googletag.enableServices(),d.forEach(function(a){googletag.display(a)})}}var x=function(a){for(var b=[],c=1;c<arguments.length;c++)b[c-1]=arguments[c];for(var d,e=0,f=b;e<f.length;e++)for(var g in d=f[e],d)a[g]=d[g];return a}({"lib":"https://cdn.sstatic.net/clc/clc.min.js?v=04d772c81312","style":"https://cdn.sstatic.net/clc/styles/clc.min.css?v=768595a6d237","u":"https://clc.stackoverflow.com/markup.js","wa":true,"kt":2000,"tto":true,"h":"clc.stackoverflow.com","allowed":"^(((talent\\.)?stackoverflow)|(blog\\.codinghorror)|(serverfault|askubuntu)|([^\\.]+\\.stackexchange))\\.com$","wv":true,"al":false,"dh":[4385506061,4386578282,4386579572]},a.options||{}),y=h(function(){var a=x.lib;l()&&(a=a.replace(/(\.min)?\.js(\?v=[0-9a-fA-F]+)?$/,'.ie$1.js$2')),i(a)}),z=a.cmd||[];Array.isArray(z)&&(0<z.length?y():k(z,y));var A=h(function(){i('https://www.googletagservices.com/tag/js/gpt.js')}),B=function(a){new Image().src='https://'+x.h+'/stalled.gif?'+a},C='248424177',D=/^\/tags\//.test(location.pathname)||/^\/questions\/tagged\//.test(location.pathname)?'tag-pages':/^\/$/.test(location.pathname)||/^\/home/.test(location.pathname)?'home-page':'question-pages',E=location.hostname;var F={slots:{lb:[[728,90]],mlb:[[728,90]],smlb:[[728,90]],bmlb:[[728,90]],sb:function(a){return'dfp-tsb'===a?[[300,250],[300,600]]:[[300,250]]},"tag-sponsorship":[[730,135]],"mobile-below-question":[[320,50],[300,250]]},ids:{"dfp-tlb":'lb',"dfp-mlb":'mlb',"dfp-smlb":'smlb',"dfp-bmlb":'bmlb',"dfp-tsb":'sb',"dfp-isb":'sb',"dfp-tag":'tag-sponsorship',"dfp-m-aq":'mobile-below-question',"clc-tlb":'lb',"clc-mlb":'mlb',"clc-tsb":'sb'}};(function(){var b=x.al;b&&z.push(function(){return a.load()})})(),n(),a.dfp={load:w},a.options=x,a.cmd=z})(this.clc=this.clc||{})}catch(a){window.console.error(a)}</script>            <script>
                var clc = clc || {};
                clc.collapse = { sb: !0,'tag-sponsorship':!0,lb:!1,mlb:!1,smlb:!1,bmlb:!1,'mobile-below-question':!0};
                clc.cmd = clc.cmd || [];
                clc.cmd.push(function () { window.clc_request='AlKbuy9nwNYIxbGdAAkAAAACAAAAAgAAAAMSAAAAfGMjfC5uZXR8ZGF0ZXRpbWV8AIJnvQT7tx2wPYQ'; clc.load(); });
                            </script>
            <div class="module sidebar-linked">
	<h4 id="h-linked">Linked</h4>
	<div class="linked" data-tracker="lq=1">

<div class="spacer">
	<a href="/q/4127363" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes answered-accepted default">66</div>
	</a>
	<a href="/questions/4127363/date-difference-in-years-using-c-sharp?noredirect=1" class="question-hyperlink">Date difference in years using C#</a>
</div>
<div class="spacer">
	<a href="/q/3152977" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes answered-accepted default">43</div>
	</a>
	<a href="/questions/3152977/calculate-the-difference-between-two-dates-and-get-the-value-in-years?noredirect=1" class="question-hyperlink">Calculate the difference between two dates and get the value in years?</a>
</div>
<div class="spacer">
	<a href="/q/2194999" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes answered-accepted default">20</div>
	</a>
	<a href="/questions/2194999/how-to-calculate-an-age-based-on-a-birthday?noredirect=1" class="question-hyperlink">How to calculate an age based on a birthday?</a>
</div>
<div class="spacer">
	<a href="/q/14766086" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes answered-accepted default">9</div>
	</a>
	<a href="/questions/14766086/how-to-calculate-age-in-years-from-dob-in-c-sharp?noredirect=1" class="question-hyperlink">How to calculate age in years from dob in c#</a>
</div>
<div class="spacer">
	<a href="/q/673476" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes  default">8</div>
	</a>
	<a href="/questions/673476/age-in-years-from-datetime-date-of-birth?noredirect=1" class="question-hyperlink">Age in years from DateTime (Date of birth)</a>
</div>
<div class="spacer">
	<a href="/q/26504044" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes answered-accepted default">4</div>
	</a>
	<a href="/questions/26504044/simple-age-calculator-base-on-date-of-birth-in-c-sharp?noredirect=1" class="question-hyperlink">Simple Age Calculator base on date of birth in C#</a>
</div>
<div class="spacer">
	<a href="/q/2237587" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes  default">2</div>
	</a>
	<a href="/questions/2237587/how-can-i-calculate-age-by-datetimepicker?noredirect=1" class="question-hyperlink">how can i calculate age by datetimepicker</a>
</div>
<div class="spacer">
	<a href="/q/14476817" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes answered-accepted default">-3</div>
	</a>
	<a href="/questions/14476817/how-to-accurately-get-difference-between-two-datetime-object-in-years-closed?noredirect=1" class="question-hyperlink">How to accurately get difference between two DateTime object in &ldquo;Years&rdquo; [Closed, use NodaTime]</a>
</div>
<div class="spacer">
	<a href="/q/2902401" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes  default">1</div>
	</a>
	<a href="/questions/2902401/calculating-age-based-on-date-of-birth-in-sql-and-c?noredirect=1" class="question-hyperlink">Calculating age based on date of birth in SQL and C#?</a>
</div>
<div class="spacer">
	<a href="/q/3889845" title="Vote score (upvotes - downvotes)">
		<div class="answer-votes answered-accepted default">1</div>
	</a>
	<a href="/questions/3889845/what-is-the-simplest-and-correct-way-to-calculate-age?noredirect=1" class="question-hyperlink">what is the simplest and correct way to calculate age?</a>
</div>					<div class="spacer more">
				<a href="/questions/linked/9">see more linked questions…</a>
			</div>
	</div>
</div>

                <div class="module sidebar-related">
                    <h4 id="h-related">Related</h4>
                    <div class="related js-gps-related-questions" data-tracker="rq=1">
                        <div class="spacer"><a href="/q/11" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">1419</div></a><a href="/questions/11/calculate-relative-time-in-c-sharp" class="question-hyperlink">Calculate relative time in C#</a></div><div class="spacer"><a href="/q/7074" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">5886</div></a><a href="/questions/7074/what-is-the-difference-between-string-and-string-in-c" class="question-hyperlink">What is the difference between String and string in C#?</a></div><div class="spacer"><a href="/q/29482" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">2816</div></a><a href="/questions/29482/cast-int-to-enum-in-c-sharp" class="question-hyperlink">Cast int to enum in C#</a></div><div class="spacer"><a href="/q/105372" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">3405</div></a><a href="/questions/105372/how-do-i-enumerate-an-enum-in-c" class="question-hyperlink">How do I enumerate an enum in C#?</a></div><div class="spacer"><a href="/q/151005" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">1707</div></a><a href="/questions/151005/how-to-create-excel-xls-and-xlsx-file-in-c-sharp-without-installing-ms-offic" class="question-hyperlink">How to create Excel (.XLS and .XLSX) file in C# without installing Ms Office?</a></div><div class="spacer"><a href="/q/221294" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">3635</div></a><a href="/questions/221294/how-do-you-get-a-timestamp-in-javascript" class="question-hyperlink">How do you get a timestamp in JavaScript?</a></div><div class="spacer"><a href="/q/247621" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">2281</div></a><a href="/questions/247621/what-are-the-correct-version-numbers-for-c" class="question-hyperlink">What are the correct version numbers for C#?</a></div><div class="spacer"><a href="/q/325933" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">1082</div></a><a href="/questions/325933/determine-whether-two-date-ranges-overlap" class="question-hyperlink">Determine Whether Two Date Ranges Overlap</a></div><div class="spacer"><a href="/q/371328" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">1295</div></a><a href="/questions/371328/why-is-it-important-to-override-gethashcode-when-equals-method-is-overridden" class="question-hyperlink">Why is it important to override GetHashCode when Equals method is overridden?</a></div><div class="spacer"><a href="/q/472906" title="Vote score (upvotes - downvotes)"><div class="answer-votes answered-accepted extra-large">2040</div></a><a href="/questions/472906/how-do-i-get-a-consistent-byte-representation-of-strings-in-c-sharp-without-manu" class="question-hyperlink">How do I get a consistent byte representation of strings in C# without manually specifying an encoding?</a></div>
                    </div>
                </div>

<div id="hot-network-questions" class="module tex2jax_ignore">
    <h4>
        <a href="https://stackexchange.com/questions?tab=hot"
           class="js-gps-track s-link s-link__inherit" 
           data-gps-track="posts_hot_network.click({ item_type:1, location:11 })">
            Hot Network Questions
        </a>
    </h4>
    <ul>
            <li >
                <div class="favicon favicon-writing" title="Writing Stack Exchange"></div><a href="https://writing.stackexchange.com/questions/44589/how-is-simplicity-better-than-precision-and-clarity-in-prose" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:166 }); posts_hot_network.click({ item_type:2, location:11 })">
                    How is simplicity better than precision and clarity in prose?
                </a>

            </li>
            <li >
                <div class="favicon favicon-apple" title="Ask Different"></div><a href="https://apple.stackexchange.com/questions/356703/create-an-outline-of-font" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:118 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Create an outline of font
                </a>

            </li>
            <li >
                <div class="favicon favicon-math" title="Mathematics Stack Exchange"></div><a href="https://math.stackexchange.com/questions/3186480/empty-set-is-subset-of-every-set-if-yes-why-that" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:69 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Empty set is subset of every set? If yes, why that...
                </a>

            </li>
            <li >
                <div class="favicon favicon-chess" title="Chess Stack Exchange"></div><a href="https://chess.stackexchange.com/questions/24183/how-do-you-keep-chess-fun-when-your-opponent-constantly-beats-you" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:435 }); posts_hot_network.click({ item_type:2, location:11 })">
                    How do you keep chess fun when your opponent constantly beats you?
                </a>

            </li>
            <li >
                <div class="favicon favicon-codegolf" title="Programming Puzzles &amp; Code Golf Stack Exchange"></div><a href="https://codegolf.stackexchange.com/questions/183014/match-roman-numerals" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:200 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Match Roman Numerals
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-skeptics" title="Skeptics Stack Exchange"></div><a href="https://skeptics.stackexchange.com/questions/43789/was-credit-for-the-black-hole-image-misattributed" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:212 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Was credit for the black hole image misattributed?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-graphicdesign" title="Graphic Design Stack Exchange"></div><a href="https://graphicdesign.stackexchange.com/questions/122512/typeface-like-times-new-roman-but-with-tied-percent-sign" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:174 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Typeface like Times New Roman but with &quot;tied&quot; percent sign
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-writing" title="Writing Stack Exchange"></div><a href="https://writing.stackexchange.com/questions/44587/scientific-reports-significant-figures" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:166 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Scientific Reports - Significant Figures
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-earthscience" title="Earth Science Stack Exchange"></div><a href="https://earthscience.stackexchange.com/questions/16726/what-are-these-gizmos-at-iza%c3%b1a-atmospheric-research-center-in-spain" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:553 }); posts_hot_network.click({ item_type:2, location:11 })">
                    What are these Gizmos at Iza&#241;a Atmospheric Research Center in Spain?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-electronics" title="Electrical Engineering Stack Exchange"></div><a href="https://electronics.stackexchange.com/questions/432355/why-is-superheterodyning-better-than-direct-conversion" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:135 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Why is superheterodyning better than direct conversion?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-space" title="Space Exploration Stack Exchange"></div><a href="https://space.stackexchange.com/questions/35503/why-does-the-event-horizon-telescope-eht-not-include-telescopes-from-africa-a" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:508 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Why does the Event Horizon Telescope (EHT) not include telescopes from Africa, Asia or Australia?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-arduino" title="Arduino Stack Exchange"></div><a href="https://arduino.stackexchange.com/questions/63446/arduino-pro-micro-switch-off-leds" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:540 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Arduino Pro Micro - switch off LEDs
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-music" title="Music: Practice &amp; Theory Stack Exchange"></div><a href="https://music.stackexchange.com/questions/82682/how-can-a-perfect-fourth-interval-be-considered-either-consonant-or-dissonant" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:240 }); posts_hot_network.click({ item_type:2, location:11 })">
                    how can a perfect fourth interval be considered either consonant or dissonant?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-askubuntu" title="Ask Ubuntu"></div><a href="https://askubuntu.com/questions/1133199/the-following-signatures-were-invalid-expkeysig-1397bc53640db551" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:89 }); posts_hot_network.click({ item_type:2, location:11 })">
                    The following signatures were invalid: EXPKEYSIG 1397BC53640DB551
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-crypto" title="Cryptography Stack Exchange"></div><a href="https://crypto.stackexchange.com/questions/68754/ipsec-esp-which-key-is-used-to-generate-the-hmac" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:281 }); posts_hot_network.click({ item_type:2, location:11 })">
                    ipsec, esp: Which key is used to generate the HMAC
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-scifi" title="Science Fiction &amp; Fantasy Stack Exchange"></div><a href="https://scifi.stackexchange.com/questions/208954/is-every-episode-of-where-are-my-pants-identical" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:186 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Is every episode of &quot;Where are my Pants?&quot; identical?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-politics" title="Politics Stack Exchange"></div><a href="https://politics.stackexchange.com/questions/40489/can-withdrawing-asylum-be-illegal" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:475 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Can withdrawing asylum be illegal?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-retrocomputing" title="Retrocomputing Stack Exchange"></div><a href="https://retrocomputing.stackexchange.com/questions/9660/what-was-the-last-x86-cpu-that-did-not-have-the-x87-floating-point-unit-built-in" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:648 }); posts_hot_network.click({ item_type:2, location:11 })">
                    What was the last x86 CPU that did not have the x87 floating-point unit built in?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-worldbuilding" title="Worldbuilding Stack Exchange"></div><a href="https://worldbuilding.stackexchange.com/questions/143727/keeping-a-retro-style-to-sci-fi-spaceships" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:579 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Keeping a retro style to sci-fi spaceships?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-worldbuilding" title="Worldbuilding Stack Exchange"></div><a href="https://worldbuilding.stackexchange.com/questions/143923/would-it-be-possible-to-rearrange-a-dragons-flight-muscle-to-somewhat-circumven" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:579 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Would it be possible to rearrange a dragon&#39;s flight muscle to somewhat circumvent the square-cube law?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-travel" title="Travel Stack Exchange"></div><a href="https://travel.stackexchange.com/questions/135609/take-groceries-in-checked-luggage" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:273 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Take groceries in checked luggage
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-physics" title="Physics Stack Exchange"></div><a href="https://physics.stackexchange.com/questions/472483/how-does-ice-melt-when-immersed-in-water" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:151 }); posts_hot_network.click({ item_type:2, location:11 })">
                    How does ice melt when immersed in water
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-worldbuilding" title="Worldbuilding Stack Exchange"></div><a href="https://worldbuilding.stackexchange.com/questions/143920/how-can-i-protect-witches-in-combat-who-wear-limited-clothing" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:579 }); posts_hot_network.click({ item_type:2, location:11 })">
                    How can I protect witches in combat who wear limited clothing?
                </a>

            </li>
            <li class="dno js-hidden">
                <div class="favicon favicon-diy" title="Home Improvement Stack Exchange"></div><a href="https://diy.stackexchange.com/questions/161945/is-this-wall-load-bearing-blueprints-and-photos-attached" class="js-gps-track question-hyperlink mb0" data-gps-track="site.switch({ item_type:11, target_site:73 }); posts_hot_network.click({ item_type:2, location:11 })">
                    Is this wall load bearing? Blueprints and photos attached
                </a>

            </li>
    </ul>

        <a href="#" 
           class="show-more js-show-more js-gps-track" 
           data-gps-track="posts_hot_network.click({ item_type:3, location:11 })">
            more hot questions
        </a>
</div>

        </div>
    </div>
<script>StackExchange.ready(function(){$.get('/posts/9/ivc/eefd');});</script>
<noscript><div><img src="/posts/9/ivc/eefd" class="dno" alt="" width="0" height="0"></div></noscript><div style="display:none" id="prettify-lang">lang-cs</div></div>


        </div>
    </div>

    <footer id="footer" class="site-footer js-footer" role="contentinfo">
        <div class="site-footer--container">
                <div class="site-footer--logo">

                    
                    <a href="https://stackoverflow.com"><svg aria-hidden="true" class="svg-icon native iconLogoGlyphMd" width="32" height="37" viewBox="0 0 32 37"><g><path fill="#BCBBBB" d="M26 33v-9h4v13H0V24h4v9z"/><path d="M23.1 25.99l.68-2.95-16.1-3.35L7 23l16.1 2.99zM9.1 15.2l15 7 1.4-3-15-7-1.4 3zm4.2-7.4L26 18.4l2.1-2.5L15.4 5.3l-2.1 2.5zM21.5 0l-2.7 2 9.9 13.3 2.7-2L21.5 0zM7 30h16v-3H7v3z" fill="#F48024"/></g></svg></a>
                </div>
            
            <nav class="site-footer--nav">
                <div class="site-footer--col site-footer--col__visible js-footer-col" data-name="default">
                    <h5 class="-title"><a href="https://stackoverflow.com" class="js-gps-track" >Stack Overflow</a></h5>
                    <ul class="-list js-primary-footer-links">
                        <li class="-item"><a href="/questions" class="js-gps-track -link" >Questions</a></li>
                            <li class="-item"><a href="https://stackoverflow.com/jobs" class="js-gps-track -link" >Jobs</a></li>
                            <li class="-item"><a href="https://stackoverflow.com/jobs/directory/developer-jobs" class="js-gps-track -link" >Developer Jobs Directory</a></li>
                                    <li class="-item"><a href="https://stackoverflow.com/jobs/salary" class="js-gps-track -link" >Salary Calculator</a></li>
                                                        <li class="-item"><a href="/help" class="js-gps-track -link" >Help</a></li>
                            <li class="-item"><a onclick='StackExchange.switchMobile("on")' class="js-gps-track -link" >Mobile</a></li>
                    </ul>
                </div>
                <div class="site-footer--col site-footer--col__visible js-footer-col" data-name="default">
                    <h5 class="-title"><a href="https://www.stackoverflowbusiness.com" class="js-gps-track" >Products</a></h5>
                    <ul class="-list">
                        <li class="-item"><a href="https://stackoverflow.com/teams" class="js-gps-track -link" 
                                                data-ga="">Teams</a></li>
                        <li class="-item"><a href="https://www.stackoverflowbusiness.com/talent" class="js-gps-track -link" >Talent</a></li>
                        <li class="-item"><a href="https://www.stackoverflowbusiness.com/advertising" class="js-gps-track -link" >Advertising</a></li>
                        <li class="-item"><a href="https://stackoverflow.com/enterprise" class="js-gps-track -link" >Enterprise</a></li>
                    </ul>
                </div>
                <div class="site-footer--col site-footer--col__visible js-footer-col" data-name="default">
                    <h5 class="-title"><a class="js-gps-track"  href="https://stackoverflow.com/company/about">Company</a></h5>
                    <ul class="-list">
                            <li class="-item"><a class="js-gps-track -link"  href="https://stackoverflow.com/company/about">About</a></li>
                                                <li class="-item"><a class="js-gps-track -link" href="https://stackoverflow.com/company/press">Press</a></li>
                            <li class="-item"><a class="js-gps-track -link"  href="https://stackoverflow.com/company/work-here">Work Here</a></li>
                        <li class="-item"><a class="js-gps-track -link"  href="https://stackoverflow.com/legal">Legal</a></li>
                        <li class="-item"><a class="js-gps-track -link"  href="https://stackoverflow.com/legal/privacy-policy">Privacy Policy</a></li>
                            <li class="-item"><a class="js-gps-track -link"  href="https://stackoverflow.com/company/contact">Contact Us</a></li>
                    </ul>
                </div>
                <div class="site-footer--col site-footer--categories-nav site-footer--col__visible">
                    <a href="#" class="site-footer--back js-footer-back"><svg aria-hidden="true" class="svg-icon iconArrowLeftAlt" width="18" height="18" viewBox="0 0 18 18"><path d="M10.58 16L12 14.59 6.4 9 12 3.41 10.57 2l-7 7z"/></svg></a>
                    <div>
                        <h5 class="-title"><a href="https://stackexchange.com">Stack Exchange<br> Network</a></h5>
                        <ul class="-list">
                            <li class="-item"><a href="#" class="-link _expandable js-footer-category-trigger js-gps-track" data-target="Technology">Technology</a></li>
                            <li class="-item"><a href="#" class="-link _expandable js-footer-category-trigger js-gps-track"  data-target="Life / Arts">Life / Arts</a></li>
                            <li class="-item"><a href="#" class="-link _expandable js-footer-category-trigger js-gps-track"  data-target="Culture / Recreation">Culture / Recreation</a></li>
                            <li class="-item"><a href="#" class="-link _expandable js-footer-category-trigger js-gps-track"  data-target="Science">Science</a></li>
                            <li class="-item"><a href="#" class="-link _expandable js-footer-category-trigger js-gps-track"  data-target="Other">Other</a></li>

                        </ul>
                    </div>
                </div>
                <div class="site-footer--categories">
                    <div class="site-footer--col site-footer--category js-footer-col" data-name="Technology">
                        <ul class="-list">
                                <li class="-item"><a href="https://stackoverflow.com" class="-link js-gps-track"  title="professional and enthusiast programmers">Stack Overflow</a></li>
                                <li class="-item"><a href="https://serverfault.com" class="-link js-gps-track"  title="system and network administrators">Server Fault</a></li>
                                <li class="-item"><a href="https://superuser.com" class="-link js-gps-track"  title="computer enthusiasts and power users">Super User</a></li>
                                <li class="-item"><a href="https://webapps.stackexchange.com" class="-link js-gps-track"  title="power users of web applications">Web Applications</a></li>
                                <li class="-item"><a href="https://askubuntu.com" class="-link js-gps-track" title="Ubuntu users and developers">Ask Ubuntu</a></li>
                                <li class="-item"><a href="https://webmasters.stackexchange.com" class="-link js-gps-track"  title="pro webmasters">Webmasters</a></li>
                                <li class="-item"><a href="https://gamedev.stackexchange.com" class="-link js-gps-track"  title="professional and independent game developers">Game Development</a></li>
                                    </ul></div><div class="site-footer--col site-footer--category js-footer-col" data-name="Technology"><ul class="-list">
                                <li class="-item"><a href="https://tex.stackexchange.com" class="-link js-gps-track" title="users of TeX, LaTeX, ConTeXt, and related typesetting systems">TeX - LaTeX</a></li>
                                <li class="-item"><a href="https://softwareengineering.stackexchange.com" class="-link js-gps-track"  title="professionals, academics, and students working within the systems development life cycle">Software Engineering</a></li>
                                <li class="-item"><a href="https://unix.stackexchange.com" class="-link js-gps-track"  title="users of Linux, FreeBSD and other Un*x-like operating systems">Unix &amp; Linux</a></li>
                                <li class="-item"><a href="https://apple.stackexchange.com" class="-link js-gps-track"  title="power users of Apple hardware and software">Ask Different (Apple)</a></li>
                                <li class="-item"><a href="https://wordpress.stackexchange.com" class="-link js-gps-track"  title="WordPress developers and administrators">WordPress Development</a></li>
                                <li class="-item"><a href="https://gis.stackexchange.com" class="-link js-gps-track"  title="cartographers, geographers and GIS professionals">Geographic Information Systems</a></li>
                                <li class="-item"><a href="https://electronics.stackexchange.com" class="-link js-gps-track"  title="electronics and electrical engineering professionals, students, and enthusiasts">Electrical Engineering</a></li>
                                    </ul></div><div class="site-footer--col site-footer--category js-footer-col" data-name="Technology"><ul class="-list">
                                <li class="-item"><a href="https://android.stackexchange.com" class="-link js-gps-track"  title="enthusiasts and power users of the Android operating system">Android Enthusiasts</a></li>
                                <li class="-item"><a href="https://security.stackexchange.com" class="-link js-gps-track"  title="information security professionals">Information Security</a></li>
                                <li class="-item"><a href="https://dba.stackexchange.com" class="-link js-gps-track"  title="database professionals who wish to improve their database skills and learn from others in the community">Database Administrators</a></li>
                                <li class="-item"><a href="https://drupal.stackexchange.com" class="-link js-gps-track"  title="Drupal developers and administrators">Drupal Answers</a></li>
                                <li class="-item"><a href="https://sharepoint.stackexchange.com" class="-link js-gps-track"  title="SharePoint enthusiasts">SharePoint</a></li>
                                <li class="-item"><a href="https://ux.stackexchange.com" class="-link js-gps-track"  title="user experience researchers and experts">User Experience</a></li>
                                <li class="-item"><a href="https://mathematica.stackexchange.com" class="-link js-gps-track"  title="users of Wolfram Mathematica">Mathematica</a></li>
                                    </ul></div><div class="site-footer--col site-footer--category js-footer-col" data-name="Technology"><ul class="-list">
                                <li class="-item"><a href="https://salesforce.stackexchange.com" class="-link js-gps-track"  title="Salesforce administrators, implementation experts, developers and anybody in-between">Salesforce</a></li>
                                <li class="-item"><a href="https://expressionengine.stackexchange.com" class="-link js-gps-track"  title="administrators, end users, developers and designers for ExpressionEngine&#174; CMS">ExpressionEngine&#174; Answers</a></li>
                                <li class="-item"><a href="https://pt.stackoverflow.com" class="-link js-gps-track"  title="programadores profissionais e entusiastas">Stack Overflow em Portugu&#234;s</a></li>
                                <li class="-item"><a href="https://blender.stackexchange.com" class="-link js-gps-track"  title="people who use Blender to create 3D graphics, animations, or games">Blender</a></li>
                                <li class="-item"><a href="https://networkengineering.stackexchange.com" class="-link js-gps-track"  title="network engineers">Network Engineering</a></li>
                                <li class="-item"><a href="https://crypto.stackexchange.com" class="-link js-gps-track"  title="software developers, mathematicians and others interested in cryptography">Cryptography</a></li>
                                <li class="-item"><a href="https://codereview.stackexchange.com" class="-link js-gps-track"  title="peer programmer code reviews">Code Review</a></li>
                                    </ul></div><div class="site-footer--col site-footer--category js-footer-col" data-name="Technology"><ul class="-list">
                                <li class="-item"><a href="https://magento.stackexchange.com" class="-link js-gps-track"  title="users of the Magento e-Commerce platform">Magento</a></li>
                                <li class="-item"><a href="https://softwarerecs.stackexchange.com" class="-link js-gps-track"  title="people seeking specific software recommendations">Software Recommendations</a></li>
                                <li class="-item"><a href="https://dsp.stackexchange.com" class="-link js-gps-track"  title="practitioners of the art and science of signal, image and video processing">Signal Processing</a></li>
                                <li class="-item"><a href="https://emacs.stackexchange.com" class="-link js-gps-track"  title="those using, extending or developing Emacs">Emacs</a></li>
                                <li class="-item"><a href="https://raspberrypi.stackexchange.com" class="-link js-gps-track"  title="users and developers of hardware and software for Raspberry Pi">Raspberry Pi</a></li>
                                <li class="-item"><a href="https://ru.stackoverflow.com" class="-link js-gps-track"  title="программистов">Stack Overflow на русском</a></li>
                                <li class="-item"><a href="https://codegolf.stackexchange.com" class="-link js-gps-track"  title="programming puzzle enthusiasts and code golfers">Programming Puzzles &amp; Code Golf</a></li>
                                    </ul></div><div class="site-footer--col site-footer--category js-footer-col" data-name="Technology"><ul class="-list">
                                <li class="-item"><a href="https://es.stackoverflow.com" class="-link js-gps-track"  title="programadores y profesionales de la inform&#225;tica">Stack Overflow en espa&#241;ol</a></li>
                                <li class="-item"><a href="https://ethereum.stackexchange.com" class="-link js-gps-track"  title="users of Ethereum, the decentralized application platform and smart contract enabled blockchain">Ethereum</a></li>
                                <li class="-item"><a href="https://datascience.stackexchange.com" class="-link js-gps-track"  title="Data science professionals, Machine Learning specialists, and those interested in learning more about the field">Data Science</a></li>
                                <li class="-item"><a href="https://arduino.stackexchange.com" class="-link js-gps-track"  title="developers of open-source hardware and software that is compatible with Arduino">Arduino</a></li>
                                <li class="-item"><a href="https://bitcoin.stackexchange.com" class="-link js-gps-track"  title="Bitcoin crypto-currency enthusiasts">Bitcoin</a></li>
                                            <li class="-item">
                                    <a href="https://stackexchange.com/sites#technology" class="-link js-gps-track" >
                                        <strong>
                                            more (31)
                                        </strong>
                                    </a>
                                </li>
                        </ul>
                    </div>
                </div>
                <div class="site-footer--col site-footer--category js-footer-col" data-name="Life / Arts">
                    <ul class="-list">
                            <li class="-item"><a href="https://photo.stackexchange.com" class="-link js-gps-track"  title="professional, enthusiast and amateur photographers">Photography</a></li>
                            <li class="-item"><a href="https://scifi.stackexchange.com" class="-link js-gps-track"  title="science fiction and fantasy enthusiasts">Science Fiction &amp; Fantasy</a></li>
                            <li class="-item"><a href="https://graphicdesign.stackexchange.com" class="-link js-gps-track"  title="Graphic Design professionals, students, and enthusiasts">Graphic Design</a></li>
                            <li class="-item"><a href="https://movies.stackexchange.com" class="-link js-gps-track"  title="movie and tv enthusiasts">Movies &amp; TV</a></li>
                            <li class="-item"><a href="https://music.stackexchange.com" class="-link js-gps-track"  title="musicians, students, and enthusiasts">Music: Practice &amp; Theory</a></li>
                            <li class="-item"><a href="https://worldbuilding.stackexchange.com" class="-link js-gps-track"  title="writers/artists using science, geography and culture to construct imaginary worlds and settings">Worldbuilding</a></li>
                            <li class="-item"><a href="https://cooking.stackexchange.com" class="-link js-gps-track"  title="professional and amateur chefs">Seasoned Advice (cooking)</a></li>
                                </ul></div><div class="site-footer--col site-footer--category js-footer-col" data-name="Life / Arts"><ul class="-list">
                            <li class="-item"><a href="https://diy.stackexchange.com" class="-link js-gps-track"  title="contractors and serious DIYers">Home Improvement</a></li>
                            <li class="-item"><a href="https://money.stackexchange.com" class="-link js-gps-track"  title="people who want to be financially literate">Personal Finance &amp; Money</a></li>
                            <li class="-item"><a href="https://academia.stackexchange.com" class="-link js-gps-track"  title="academics and those enrolled in higher education">Academia</a></li>
                            <li class="-item"><a href="https://law.stackexchange.com" class="-link js-gps-track"  title="legal professionals, students, and others with experience or interest in law">Law</a></li>
                                        <li class="-item">
                                <a href="https://stackexchange.com/sites#lifearts" class="-link js-gps-track" >
                                    <strong>
                                        more (15)
                                    </strong>
                                </a>
                            </li>
                    </ul>
                </div>
                <div class="site-footer--col site-footer--category js-footer-col" data-name="Culture / Recreation">
                    <ul class="-list">
                            <li class="-item"><a href="https://english.stackexchange.com" class="-link js-gps-track"  title="linguists, etymologists, and serious English language enthusiasts">English Language &amp; Usage</a></li>
                            <li class="-item"><a href="https://skeptics.stackexchange.com" class="-link js-gps-track"  title="scientific skepticism">Skeptics</a></li>
                            <li class="-item"><a href="https://judaism.stackexchange.com" class="-link js-gps-track"  title="those who base their lives on Jewish law and tradition and anyone interested in learning more">Mi Yodeya (Judaism)</a></li>
                            <li class="-item"><a href="https://travel.stackexchange.com" class="-link js-gps-track"  title="road warriors and seasoned travelers">Travel</a></li>
                            <li class="-item"><a href="https://christianity.stackexchange.com" class="-link js-gps-track"  title="committed Christians, experts in Christianity and those interested in learning more">Christianity</a></li>
                            <li class="-item"><a href="https://ell.stackexchange.com" class="-link js-gps-track"  title="speakers of other languages learning English">English Language Learners</a></li>
                            <li class="-item"><a href="https://japanese.stackexchange.com" class="-link js-gps-track"  title="students, teachers, and linguists wanting to discuss the finer points of the Japanese language">Japanese Language</a></li>
                                </ul></div><div class="site-footer--col site-footer--category js-footer-col" data-name="Culture / Recreation"><ul class="-list">
                            <li class="-item"><a href="https://gaming.stackexchange.com" class="-link js-gps-track"  title="passionate videogamers on all platforms">Arqade (gaming)</a></li>
                            <li class="-item"><a href="https://bicycles.stackexchange.com" class="-link js-gps-track"  title="people who build and repair bicycles, people who train cycling, or commute on bicycles">Bicycles</a></li>
                            <li class="-item"><a href="https://rpg.stackexchange.com" class="-link js-gps-track"  title="gamemasters and players of tabletop, paper-and-pencil role-playing games">Role-playing Games</a></li>
                            <li class="-item"><a href="https://anime.stackexchange.com" class="-link js-gps-track"  title="anime and manga fans">Anime &amp; Manga</a></li>
                            <li class="-item"><a href="https://puzzling.stackexchange.com" class="-link js-gps-track"  title="those who create, solve, and study puzzles">Puzzling</a></li>
                            <li class="-item"><a href="https://mechanics.stackexchange.com" class="-link js-gps-track"  title="mechanics and DIY enthusiast owners of cars, trucks, and motorcycles">Motor Vehicle Maintenance &amp; Repair</a></li>
                                        <li class="-item">
                                <a href="https://stackexchange.com/sites#culturerecreation" class="-link js-gps-track" >
                                    <strong>
                                        more (33)
                                    </strong>
                                </a>
                            </li>
                    </ul>
                </div>
            
            </nav>

            </footer>
            
    </body>
    </html>
