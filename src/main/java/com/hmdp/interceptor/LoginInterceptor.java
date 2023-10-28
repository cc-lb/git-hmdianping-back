package com.hmdp.interceptor;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.utils.UserHolder;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * @Classname LoginInterceptor
 * @Description
 * @Date 2023/6/13 20:29
 * @Created by cc
 */
public class LoginInterceptor implements HandlerInterceptor {


    /**
     * 校验用户状态
     * 1，看是否存在token
     * 2，
     * @param request
     * @param response
     * @param handler
     * @return
     * @throws Exception
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
/*
        HttpSession session = request.getSession();
        Object user = session.getAttribute("user");

        if(user==null) {
            response.setStatus(401);
            return  false;
        }
        //多线程，存在ThreadLocal里面
        UserHolder.saveUser((UserDTO) user);

        */

        if(UserHolder.getUser()==null) {
            response.setStatus(401);
        return  false;
        }
            return true;
       // return HandlerInterceptor.super.preHandle(request, response, handler);
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        HandlerInterceptor.super.postHandle(request, response, handler, modelAndView);
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        HandlerInterceptor.super.afterCompletion(request, response, handler, ex);
    }
}
